"""Official current rosters plus explicitly dated secondary archive snapshots.

No inferred effective dates. A complete chamber is validated before publication.
Raw evidence is retained on each changed snapshot. Failures never replace rosters.
"""
from __future__ import annotations
import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import unicodedata
from xml.etree import ElementTree as ET

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

HOUSE = 'https://clerk.house.gov/xml/lists/MemberData.xml'
SENATORS = 'https://www.senate.gov/legislative/LIS_MEMBER/cvc_member_data.xml'
SENATE_INDEX = 'https://www.senate.gov/pagelayout/committees/b_three_sections_with_teasers/membership.htm'
REPO = 'https://api.github.com/repos/unitedstates/congress-legislators'
RAW = 'https://raw.githubusercontent.com/unitedstates/congress-legislators'


def fetch(url):
    with requests.Session() as session:
        session.mount('https://', HTTPAdapter(max_retries=Retry(total=3, backoff_factor=1, status_forcelist=[429,500,502,503,504])))
        response = session.get(url, timeout=(15,60), headers={'User-Agent':'Vail committee research (vail.finance)'})
        response.raise_for_status()
        return response.text


def txt(node, path):
    return ' '.join((node.findtext(path) or '').split())


def normalized(s):
    return ''.join(c for c in unicodedata.normalize('NFKD',s).lower() if c.isalnum())


def congress_on(date):
    year = date.year - (date.month == 1 and date.day < 3)
    return (year - 1789)//2 + 1


def document(url, content):
    return {'url':url,'sha256':hashlib.sha256(content.encode()).hexdigest(),'content':content}


def snapshot(key, congress, committees, members, assignments, documents, observed=None, archive=False, version=None):
    observed = observed or datetime.now(timezone.utc).isoformat()
    payload = {'source_key':key, 'congress':congress, 'committees':committees, 'members':members,
               'assignments':assignments, 'documents':documents, 'observed_at':observed,
               'evidence_kind':'archive_snapshot' if archive else 'official_current'}
    # Stable across observations, changes whenever any source evidence changes.
    payload['source_version'] = version or hashlib.sha256(json.dumps([committees,members,assignments,[d['sha256'] for d in documents]],sort_keys=True).encode()).hexdigest()
    validate(payload)
    return payload


def validate(p):
    committees = {c['id'] for c in p['committees']}
    members = {m['id'] for m in p['members']}
    if len(committees) != len(p['committees']) or len(members) != len(p['members']):
        raise ValueError('Duplicate committee/member identity')
    seen = set()
    for a in p['assignments']:
        key = (a['committee_id'],a['member_id'])
        if key in seen or a['committee_id'] not in committees or a['member_id'] not in members:
            raise ValueError(f'Duplicate or unresolved assignment: {key}')
        if not re.fullmatch(r'[A-Z]\d{6}',a['member_id']) or a['chamber'] not in ('House','Senate'):
            raise ValueError('Invalid stable member identity/chamber')
        seen.add(key)
    if not committees or (not seen and p['evidence_kind']=='official_current'):
        raise ValueError('Empty roster')
    if p['evidence_kind']=='official_current':
        floor = 1500 if p['source_key']=='official_house' else 400
        if len(seen) < floor:
            raise ValueError(f'Incomplete current roster: {len(seen)} assignments')


def parse_house(content):
    root=ET.fromstring(content)
    congress=int(txt(root,'title-info/congress-num'))
    if congress != congress_on(datetime.now(timezone.utc)):
        raise ValueError('House roster is not for current Congress')
    committees=[]; members=[]; assignments=[]
    for c in root.findall('./committees/committee'):
        cid='house:'+c.attrib['comcode']
        committees.append({'id':cid,'name':txt(c,'committee-fullname'),'parent_id':None,'source_url':HOUSE})
        for sub in c.findall('subcommittee'):
            committees.append({'id':'house:'+sub.attrib['subcomcode'],'name':txt(sub,'subcommittee-fullname'),'parent_id':cid,'source_url':HOUSE})
    for member in root.findall('./members/member'):
        mid=txt(member,'member-info/bioguideID')
        if not mid: continue  # Explicit vacancies do not have an identity.
        members.append({'id':mid,'name':txt(member,'member-info/official-name'),'chamber':'House',
                        'first_name':txt(member,'member-info/firstname'),'last_name':txt(member,'member-info/lastname'),
                        'state':(member.find('./member-info/state').attrib.get('postal-code','') if member.find('./member-info/state') is not None else ''),
                        'party':txt(member,'member-info/party'),'source_url':HOUSE})
        for a in member.findall('./committee-assignments/*'):
            code=a.attrib.get('comcode') or a.attrib.get('subcomcode')
            if not code:
                if a.tag == 'committee' and dict(a.attrib) == {'rank':''}: continue
                raise ValueError('Unknown House assignment shape')
            assignments.append({'committee_id':'house:'+code,'member_id':mid,'chamber':'House','role':a.attrib.get('leadership') or 'Member'})
    if len(members)<430: raise ValueError('Incomplete House member roster')
    return snapshot('official_house',congress,committees,members,assignments,[document(HOUSE,content)])


def parse_senate(roster, index, sources):
    root=ET.fromstring(roster); members=[]; lookup={}
    for s in root.findall('.//senator'):
        mid=txt(s,'bioguideId'); key=(normalized(txt(s,'name/last')),txt(s,'state'))
        if key in lookup: raise ValueError('Ambiguous Senate identity')
        lookup[key]=mid
        members.append({'id':mid,'name':txt(s,'name/first')+' '+txt(s,'name/last'),'chamber':'Senate',
                        'first_name':txt(s,'name/first'),'last_name':txt(s,'name/last'),
                        'state':txt(s,'state'),'party':txt(s,'party'),'source_url':SENATORS})
    if len(members)<98: raise ValueError('Incomplete Senate member roster')
    expected=set(re.findall(r'committee_memberships_([A-Z]{4})\.htm',index))
    expected.update(c.attrib['code'][:4] for c in root.findall('.//committees/committee'))
    if len(expected)<20 or expected != set(sources): raise ValueError('Incomplete Senate committee index')
    committees=[]; assignments=[]; documents=[document(SENATORS,roster),document(SENATE_INDEX,index)]
    for code,content in sorted(sources.items()):
        url=f'https://www.senate.gov/general/committee_membership/committee_memberships_{code}.xml'
        documents.append(document(url,content))
        c=ET.fromstring(content).find('committees')
        if c is None or txt(c,'committee_code')[:4]!=code: raise ValueError('Unexpected Senate committee document')
        for node in [c,*c.findall('subcommittee')]:
            cid='senate:'+txt(node,'committee_code')
            name=txt(node,'committee_name') or txt(node,'subcommittee_name')
            if not name: raise ValueError('Missing committee name')
            committees.append({'id':cid,'name':name,'parent_id':None if node is c else 'senate:'+txt(c,'committee_code'),'source_url':url})
            # Some joint/advisory committees explicitly have no Senate appointments.
            for m in node.findall('./members/member'):
                key=(normalized(txt(m,'name/last')),txt(m,'state'))
                if key not in lookup: raise ValueError(f'Unresolved Senate identity: {key}')
                role=txt(m,'position') or 'Member'
                assignments.append({'committee_id':cid,'member_id':lookup[key],'chamber':'Senate','role':'Ranking Member' if role=='Ranking' else role})
    # Cross-check every current senator's full committee listing against roster pages.
    actual={(a['member_id'],a['committee_id']) for a in assignments}
    for s in root.findall('.//senator'):
        for c in s.findall('./committees/committee'):
            if (txt(s,'bioguideId'),'senate:'+c.attrib['code']) not in actual:
                raise ValueError(f"Senate sources disagree: {txt(s,'bioguideId')} {c.attrib['code']}")
    return snapshot('official_senate',congress_on(datetime.now(timezone.utc)),committees,members,assignments,documents)


def current_senate():
    roster=fetch(SENATORS); index=fetch(SENATE_INDEX)
    codes=sorted(set(re.findall(r'committee_memberships_([A-Z]{4})\.htm',index)) | {c.attrib['code'][:4] for c in ET.fromstring(roster).findall('.//committees/committee')})
    with ThreadPoolExecutor(max_workers=4) as pool:
        bodies=list(pool.map(lambda code: fetch(f'https://www.senate.gov/general/committee_membership/committee_memberships_{code}.xml'),codes))
    return parse_senate(roster,index,dict(zip(codes,bodies)))


def archived_snapshots(since='2024-01-01'):
    """All membership revisions since the cutoff; baseline is labeled with its real date.

    These are community-maintained archived observations, NOT official effective dates.
    """
    import yaml
    commits=[]; page=1
    while True:
        batch=json.loads(fetch(f'{REPO}/commits?path=committee-membership-current.yaml&since={since}T00:00:00Z&per_page=100&page={page}'))
        commits.extend(batch)
        if len(batch)<100: break
        page+=1
    baseline=json.loads(fetch(f'{REPO}/commits?path=committee-membership-current.yaml&until={since}T00:00:00Z&per_page=1'))
    unique={c['sha']:c for c in [*commits,*baseline]}
    for commit in sorted(unique.values(),key=lambda c:(c['commit']['committer']['date'],c['sha'])):
        sha=commit['sha']; observed=commit['commit']['committer']['date']
        urls=[f'{RAW}/{sha}/{name}' for name in ['committees-current.yaml','committee-membership-current.yaml']]
        bodies=[fetch(u) for u in urls]
        catalog,rosters=[yaml.safe_load(b) for b in bodies]
        committees=[]; members={}; assignments=[]
        for c in catalog:
            cid=c['thomas_id']
            committees.append({'id':cid,'name':c['name'],'parent_id':None,'source_url':urls[0]})
            for sub in c.get('subcommittees',[]):
                committees.append({'id':cid+sub['thomas_id'],'name':sub['name'],'parent_id':cid,'source_url':urls[0]})
        for cid,rows in rosters.items():
            for a in rows:
                mid=a['bioguide']; chamber=a.get('chamber') or ('house' if cid.startswith('H') else 'senate' if cid.startswith('S') else '')
                if chamber not in ('house','senate'): raise ValueError('Archive joint membership missing chamber')
                members[mid]={'id':mid,'name':a['name'],'chamber':chamber.title()}
                assignments.append({'committee_id':cid,'member_id':mid,'chamber':chamber.title(),'role':a.get('title') or 'Member'})
        # Congressional turnover can leave mixed rosters in upstream snapshots. The
        # label identifies the archive's observation Congress, not membership tenure.
        yield snapshot('archive_unitedstates',congress_on(datetime.fromisoformat(observed.replace('Z','+00:00'))),committees,list(members.values()),assignments,
                       [document(u,b) for u,b in zip(urls,bodies)],observed,True,sha)


def run_source(client,key,produce,output):
    run=None
    if client:
        run=client.table('committee_sync_runs').insert({'source_key':key,'status':'running'}).execute().data[0]['id']
    try:
        count=0
        for p in produce():
            output.mkdir(parents=True,exist_ok=True)
            (output/f"{key}-{p['source_version']}.json").write_text(json.dumps(p))
            if client: client.rpc('publish_committee_snapshot',{'p':p}).execute()
            print(f"{key}: {len(p['committees'])} committees, {len(p['assignments'])} assignments, observed {p['observed_at']}")
            count+=1
        if not count: raise ValueError('No snapshots produced')
        if client: client.table('committee_sync_runs').update({'status':'success','finished_at':datetime.now(timezone.utc).isoformat(),'detail':f'{count} snapshots verified'}).eq('id',run).execute()
        return True
    except Exception as exc:
        # No credentials or response bodies are logged.
        detail=f'{type(exc).__name__}: {str(exc)[:300]}'
        print(f'{key} FAILED: {detail}')
        if client and run: client.table('committee_sync_runs').update({'status':'failed','finished_at':datetime.now(timezone.utc).isoformat(),'detail':detail}).eq('id',run).execute()
        return False


def main():
    from dotenv import load_dotenv
    load_dotenv('.env.local')
    parser=argparse.ArgumentParser(); parser.add_argument('--write',action='store_true'); parser.add_argument('--backfill',action='store_true')
    parser.add_argument('--output',default='artifacts/committee-research/snapshots'); args=parser.parse_args()
    client=None
    if args.write:
        from supabase import create_client
        client=create_client(os.environ['SUPABASE_URL'],os.environ['SUPABASE_SERVICE_KEY'])
    output=Path(args.output)
    jobs=[('archive_unitedstates',lambda:archived_snapshots())] if args.backfill else [('official_house',lambda:[parse_house(fetch(HOUSE))]),('official_senate',lambda:[current_senate()])]
    results=[run_source(client,key,produce,output) for key,produce in jobs]
    return 0 if all(results) else 1

if __name__=='__main__': raise SystemExit(main())
