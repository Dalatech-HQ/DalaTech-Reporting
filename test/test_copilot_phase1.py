import os
import tempfile
import unittest

import app as app_module
from modules.data_store import DataStore


class CopilotPhase1Tests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmpdir.name, 'copilot_test.db')
        self.ds = DataStore(self.db_path)
        self.original_ds = app_module.ds
        app_module.ds = self.ds
        self.client = app_module.app.test_client()
        self.report_id = self.ds.save_report(
            '2026-03-01',
            '2026-03-31',
            'march.xlsx',
            total_revenue=125000,
            total_qty=320,
            total_stores=14,
            brand_count=1,
        )
        self.ds.save_brand_kpis(
            self.report_id,
            'Zayith',
            {
                'total_revenue': 125000,
                'total_qty': 320,
                'num_stores': 14,
                'repeat_pct': 22.5,
                'stock_days_cover': 4.0,
                'inv_health_status': 'At Risk',
                'top_store_name': 'Store 101',
                'top_store_revenue': 28000,
            },
            perf_score_dict={'grade': 'B', 'total': 71, 'revenue_score': 18, 'loyalty_score': 14, 'reach_score': 20, 'activity_score': 19},
        )
        self.ds.create_agent_action(
            agent_type='Brand Health Agent',
            subject_type='brand',
            subject_key='Zayith',
            title='Protect Zayith stock position',
            reason='Stock cover is down to 4 days.',
            report_id=self.report_id,
            priority='high',
            action_signature='phase1:zayith:stock',
        )
        self.ds.save_agent_memory(
            scope_type='brand',
            scope_key='Zayith',
            memory_text='Zayith requires stock follow-up and has a pinned recovery note.',
            memory_kind='operator_note',
            confidence=0.9,
            source='unit_test',
            memory_layer='workspace',
            tags=['copilot', 'stock'],
            related_report_id=self.report_id,
            related_brand='Zayith',
            pinned=True,
        )

    def tearDown(self):
        app_module.ds = self.original_ds
        try:
            self.tmpdir.cleanup()
        except PermissionError:
            pass

    def test_query_contract_includes_phase1_fields(self):
        response = self.client.post('/api/copilot/query', json={'question': 'Summarize Zayith and what needs attention', 'brand_name': 'Zayith'})
        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload['success'])
        for key in ('answer', 'resolved_context', 'suggested_actions', 'planned_steps', 'execution_result', 'memory_refs', 'next_jobs'):
            self.assertIn(key, payload)
        self.assertEqual(payload['resolved_context']['brand_name'], 'Zayith')
        self.assertGreaterEqual(len(payload['memory_refs']), 1)
        self.assertIn(payload['execution_state'], {'completed', 'waiting', 'failed'})

    def test_execute_requires_confirmation_for_destructive_actions(self):
        response = self.client.post('/api/copilot/execute', json={'tool_name': 'delete_report', 'arguments': {'report_id': self.report_id}})
        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload['execution_result']['status'], 'needs_confirmation')

    def test_memory_endpoint_and_pinning_work(self):
        list_response = self.client.get('/api/copilot/memory?subject_type=brand&subject_key=Zayith&limit=5')
        self.assertEqual(list_response.status_code, 200)
        items = list_response.get_json()['items']
        self.assertTrue(items)
        memory_id = items[0]['id']

        pin_response = self.client.post('/api/copilot/memory/pin', json={'memory_id': memory_id, 'pinned': False})
        self.assertEqual(pin_response.status_code, 200)
        self.assertFalse(pin_response.get_json()['memory']['pinned'])

    def test_schedule_endpoints_create_and_run_job(self):
        create_response = self.client.post('/api/copilot/schedules', json={
            'label': 'Daily Review Loop',
            'job_type': 'assistant',
            'cadence': 'daily',
            'payload': {'action': 'review'},
            'idempotency_key': 'schedule-daily-review',
        })
        self.assertEqual(create_response.status_code, 200)
        create_payload = create_response.get_json()
        self.assertEqual(create_payload['execution_result']['status'], 'success')
        job_id = create_payload['execution_result']['data']['job']['id']

        list_response = self.client.get('/api/copilot/schedules?status=all&limit=10')
        self.assertEqual(list_response.status_code, 200)
        self.assertTrue(any(item['id'] == job_id for item in list_response.get_json()['items']))

        run_response = self.client.post(f'/api/copilot/schedules/{job_id}/run-now', json={})
        self.assertEqual(run_response.status_code, 200)
        self.assertIn(run_response.get_json()['execution_result']['status'], {'success', 'error'})

    def test_connectors_endpoint_lists_phase1_connectors(self):
        response = self.client.get('/api/copilot/connectors')
        self.assertEqual(response.status_code, 200)
        connectors = response.get_json()['execution_result']['data']['connectors']
        connector_names = {item['connector'] for item in connectors}
        self.assertTrue({'drive', 'sheets', 'email', 'whatsapp', 'webhook'}.issubset(connector_names))


if __name__ == '__main__':
    unittest.main()
