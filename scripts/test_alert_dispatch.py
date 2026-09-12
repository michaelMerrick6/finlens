import unittest
from unittest.mock import patch

import alert_delivery_support as delivery
import dispatch_email_alerts as email


class Database:
    def __init__(self):
        self.rows = [{'id':'delivery-1','status':'pending','attempts':0,'channel':'email',
                      'destination':'test@example.invalid','subscription_id':'sub-1',
                      'signal_events':{'title':'Test'},'payload':{}}]
        self.sub = {'id':'sub-1','active':True,'channel':'email','destination':'test@example.invalid'}
        self.fail_sent = False
        self.fail_subscription = False

    def table(self, table):
        db = self
        class Query:
            def __init__(self):self.filters=[];self.update_value=None
            def select(self,*args,**kwargs):return self
            def eq(self,k,v):self.filters.append((k,v));return self
            def order(self,*args,**kwargs):return self
            def limit(self,*args,**kwargs):return self
            def maybe_single(self):return self
            def update(self,value):self.update_value=value;return self
            def execute(self):
                if table=='alert_subscriptions':
                    if db.fail_subscription:raise RuntimeError('DB unavailable')
                    data=db.sub
                else:
                    data=[r for r in db.rows if all(r.get(k)==v for k,v in self.filters)]
                    if self.update_value:
                        if db.fail_sent and self.update_value.get('status')=='sent':raise RuntimeError('DB unavailable')
                        for row in data:row.update(self.update_value)
                    data=[dict(r) for r in data]
                return type('Response',(),{'data':data})()
        return Query()


class DispatchTests(unittest.TestCase):
    def run_dispatch(self, db, send):return delivery.dispatch_channel(db,channel='email',batch_size=20,send=send)

    def test_overlapping_workers_send_only_once(self):
        db=Database();sent=[]
        def send(*args):
            sent.append(args)
            self.run_dispatch(db,lambda *args:self.fail('Second worker sent duplicate'))
        result=self.run_dispatch(db,send)
        self.assertEqual(result['deliveries_sent'],1)
        self.assertEqual(len(sent),1)
        self.assertEqual(db.rows[0]['status'],'sent')
        self.assertEqual(db.rows[0]['attempts'],1)

    def test_stale_claim_loses_compare_and_set(self):
        db=Database();row=dict(db.rows[0])
        self.assertTrue(delivery.claim_delivery(db,row))
        self.assertFalse(delivery.claim_delivery(db,row))

    def test_disabled_or_changed_subscription_is_not_sent(self):
        for update in [{'active':False},{'destination':'changed@example.invalid'},{'channel':'sms'}]:
            db=Database();db.sub.update(update)
            self.run_dispatch(db,lambda *args:self.fail('Stale subscription sent'))
            self.assertEqual(db.rows[0]['status'],'cancelled')

    def test_subscription_lookup_failure_remains_retryable(self):
        db=Database();db.fail_subscription=True
        with self.assertRaises(RuntimeError):self.run_dispatch(db,lambda *args:self.fail('Sent'))
        self.assertEqual(db.rows[0]['status'],'pending')

    def test_rate_limit_defers_without_losing_or_replaying_rest_of_batch(self):
        db=Database();db.rows.append(dict(db.rows[0],id='delivery-2'))
        def limited(*args):raise delivery.DeliveryRateLimited('429')
        result=self.run_dispatch(db,limited)
        self.assertEqual(result['deliveries_deferred'],1)
        self.assertEqual([r['status'] for r in db.rows],['pending','pending'])
        self.assertEqual([r['attempts'] for r in db.rows],[1,0])

    def test_email_recognizes_explicit_rate_limit(self):
        with patch.object(email,'RESEND_API_KEY','test'),patch.object(email,'RESEND_FROM_EMAIL','sender@example.invalid'),patch.object(email.requests,'post') as post:
            post.return_value.status_code=429
            with self.assertRaises(delivery.DeliveryRateLimited):
                email.send_email('test@example.invalid',{'title':'Test'},'delivery-1')

    def test_timeout_is_not_automatically_resent(self):
        db=Database()
        def timeout(*args):raise TimeoutError('May already have been accepted')
        result=self.run_dispatch(db,timeout)
        self.assertEqual(result['deliveries_uncertain'],1)
        self.assertEqual(db.rows[0]['status'],'uncertain')
        self.run_dispatch(db,lambda *args:self.fail('Ambiguous send retried'))

    def test_sent_record_failure_does_not_resend(self):
        db=Database();db.fail_sent=True;sent=[]
        with self.assertRaises(RuntimeError):self.run_dispatch(db,lambda *args:sent.append(args))
        self.assertEqual(db.rows[0]['status'],'sending')
        self.run_dispatch(db,lambda *args:self.fail('Already accepted send retried'))
        self.assertEqual(len(sent),1)

    def test_missing_config_never_mutates_queue(self):
        db=Database()
        with self.assertRaises(RuntimeError):
            delivery.dispatch_channel(db,channel='email',batch_size=20,send=lambda *args:self.fail('Sent'),configured=False)
        self.assertEqual(db.rows[0]['status'],'pending')
        self.assertEqual(db.rows[0]['attempts'],0)
        db.rows=[]
        self.assertEqual(delivery.dispatch_channel(db,channel='email',batch_size=20,send=None,configured=False)['deliveries_seen'],0)

    def test_email_has_stable_provider_idempotency_key(self):
        with patch.object(email,'RESEND_API_KEY','test'),patch.object(email,'RESEND_FROM_EMAIL','sender@example.invalid'),patch.object(email.requests,'post') as post:
            post.return_value.status_code=200
            email.send_email('test@example.invalid',{'title':'<Test>'},'delivery-1')
            self.assertEqual(post.call_args.kwargs['headers']['Idempotency-Key'],'vail-delivery-delivery-1')
            self.assertIn('&lt;Test&gt;',post.call_args.kwargs['json']['html'])


if __name__=='__main__':unittest.main()
