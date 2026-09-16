import unittest
from unittest.mock import MagicMock, patch
from daily_email_digest import digest_events, render_digest, dispatch_daily_digests
from alert_delivery_support import DeliveryRateLimited
import dispatch_email_alerts as email


def event(i, actor='Nancy Pelosi', ticker='NVDA'):
    return dict(id=str(i),source='congress',source_document_id=f'filing-{i}',signal_type='politician_trade',
                actor_name=actor,ticker=ticker,direction='buy',title=f'Trade {i}',summary='A disclosed purchase',
                payload={'asset_name':ticker,'amount_range':'$1,001 - $15,000','transaction_date':'2026-09-15'})

class DailyEmailTests(unittest.TestCase):
    def test_four_pelosi_trades_and_stock_match_make_one_complete_email(self):
        events=[event(i) for i in range(4)]+[event(4,'Ro Khanna','AMZN'),event(0)]
        rows=digest_events(None,events)
        content=render_digest({},rows)
        self.assertEqual(len(content['items']),5)
        self.assertIn('5 updates',content['subject'])
        self.assertEqual(len([i for i in content['items'] if i['actor']=='Nancy Pelosi']),4)
        self.assertIn('Ro Khanna',content['text'])

    def test_summary_expands_all_rows_not_only_ten_preview_entries(self):
        rows=[event(i) for i in range(15)]
        summary=dict(id='summary',signal_type='politician_filing_summary',payload={'summary_event_ids':[r['id'] for r in rows]})
        db=MagicMock();db.table.return_value.select.return_value.in_.return_value.execute.return_value.data=rows
        result=digest_events(db,[summary,rows[0]])
        self.assertEqual(len(result),15)
        self.assertEqual(len(render_digest({},result)['items']),15)

    def test_unavailable_summary_child_never_sends_partial_digest(self):
        db=MagicMock();db.table.return_value.select.return_value.in_.return_value.execute.return_value.data=[]
        with self.assertRaisesRegex(ValueError,'unavailable'):
            digest_events(db,[dict(id='summary',signal_type='politician_filing_summary',payload={'summary_event_ids':['missing']})])

    def test_content_escapes_source_text_and_rejects_non_web_links(self):
        row=event(1,actor='<script>');row['source_url']='javascript:alert(1)'
        result=render_digest({},[row])
        self.assertIn('&lt;script&gt;',result['html'])
        self.assertNotIn('javascript:',result['html'])
        self.assertEqual(result['items'][0]['sourceUrl'],'')

    def db(self):
        db=MagicMock()
        claims=iter([dict(id='digest',destination='test@example.invalid',events=[event(1)]),None])
        def rpc(name,args):
            response=MagicMock()
            response.execute.return_value.data=next(claims) if name=='claim_daily_email_digest' else args['p_events'] if name=='daily_email_unseen_events' else True
            return response
        db.rpc.side_effect=rpc
        return db

    def test_one_send_with_immutable_prepared_content(self):
        db=self.db();send=MagicMock(return_value='provider-id')
        result=dispatch_daily_digests(db,send=send,configured=True,min_send_interval=0)
        self.assertEqual(result['digests_sent'],1)
        self.assertEqual(send.call_count,1)
        names=[call.args[0] for call in db.rpc.call_args_list]
        self.assertEqual(names[:4],['claim_daily_email_digest','daily_email_unseen_events','prepare_daily_email_digest','finish_daily_email_digest'])
        self.assertEqual(db.rpc.call_args_list[3].args[1]['p_provider_id'],'provider-id')

    def test_provider_timeout_is_uncertain_and_rate_limit_retryable(self):
        for error,status in [(TimeoutError(),'uncertain'),(DeliveryRateLimited(),'ready')]:
            db=self.db()
            dispatch_daily_digests(db,send=MagicMock(side_effect=error),configured=True,min_send_interval=0)
            finish=next(call for call in db.rpc.call_args_list if call.args[0]=='finish_daily_email_digest')
            self.assertEqual(finish.args[1]['p_status'],status)

    def test_missing_provider_config_does_not_claim(self):
        db=self.db()
        with self.assertRaises(RuntimeError): dispatch_daily_digests(db,send=None,configured=False)
        db.rpc.assert_not_called()

    def test_resend_uses_digest_id_for_idempotency(self):
        with patch.object(email,'RESEND_API_KEY','test'),patch.object(email,'RESEND_FROM_EMAIL','sender@example.invalid'),patch.object(email.requests,'post') as post:
            post.return_value.status_code=200;post.return_value.json.return_value={'id':'provider-id'}
            self.assertEqual(email.send_digest('test@example.invalid',{'subject':'Daily','html':'<p>All trades</p>','text':'All trades'},'abc'),'provider-id')
            self.assertEqual(post.call_args.kwargs['headers']['Idempotency-Key'],'vail-daily-digest-abc')

if __name__=='__main__': unittest.main()
