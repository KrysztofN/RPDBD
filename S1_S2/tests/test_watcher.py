import sqlite3
from datetime import datetime,timezone,timedelta
from types import SimpleNamespace
from s2_incident_watcher import connect_state, record, phase_of, event_payload

D=lambda h:datetime(2018,4,1,h,tzinfo=timezone.utc)

def inc(hours=2):
    return SimpleNamespace(region='0206',crop='kukurydza_ziemniaki',
      start=D(0),end=D(hours-1),hours=hours,minimum=-3.8,threshold=0.0,event_id='a'*24)

def test_lifecycle(tmp_path):
    db=connect_state(str(tmp_path/'x.sqlite'))
    a=inc(2)
    assert phase_of(a,D(2),3)=='open'
    assert phase_of(a,D(5),3)=='closed'
    assert record(db,a,'open')[0]=='open'
    assert record(db,inc(3),'open')[0]=='updated'
    assert record(db,inc(3),'closed')[0]=='closed'
    assert db.execute('select count(*) from audit').fetchone()[0]==3
    db.close()

def test_event_contract():
    e=event_payload(inc())
    assert e['event_id']=='a'*24
    assert e['historical'] is True
    assert e['hours']==2 and e['end']=='2018-04-01 02:00 UTC'
