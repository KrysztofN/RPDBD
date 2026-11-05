"""Static safeguards. Spark integration must still be run on the real 4.0.4 cluster."""
from pathlib import Path
import ast
import unittest
ROOT=Path(__file__).resolve().parents[1]
class TestContract(unittest.TestCase):
    def test_python_syntax(self):
        for f in [*ROOT.glob('worker/*.py'),*ROOT.glob('master/*.py')]:
            ast.parse(f.read_text(),filename=str(f))
    def test_only_county(self):
        bot=(ROOT/'master/telegram_bot.py').read_text()
        self.assertIn("scope={sql_str('county:' + rid)}",bot)
        self.assertIn("S1S2_INCIDENTS_TEST_ONLY",bot)
        self.assertIn("'S1','S2'",bot)
    def test_kafka_not_clickhouse_incident_input(self):
        code=(ROOT/'worker/spark_incidents.py').read_text()
        self.assertIn("option('subscribe', IN_TOPIC)",code)
        self.assertIn("F.session_window('event_ts', gap)",code)
        self.assertIn(".option('topic', OUT_TOPIC).save()",code)
if __name__=='__main__': unittest.main()
