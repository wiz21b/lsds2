import json
import time
from datetime import datetime
import requests
from starter_code.withoutksp import allocate_flight_computers, commandline_args, readout_state, execute_action, actions


from utils import call_peer

r = None
timestep = 0

flight_computers = allocate_flight_computers(commandline_args(), timestep)
leader = flight_computers[0]

state = readout_state(timestep)
timestep += 1


def test_action(action, timestep):
    TOLERANCE = 50
    for i in range(2*TOLERANCE):
        fail = False
        expected_action = actions[max(0,timestep+i-TOLERANCE)]
        for k in action.keys():
            if k in expected_action and action[k] != expected_action[k]:
                fail = True
                break

        if not fail:
            return True

    assert False

def run():
    start_time = datetime.now()

    nb_decision = 0
    while True:
        delta = datetime.now() - start_time
        delta = delta.seconds + delta.microseconds/1000000
        timestep = int(delta / 0.001) + 1

        if timestep % 100 == 0:
            dec_per_sec = nb_decision / delta
            ts_per_sec = timestep / delta
            print(f"Timestep :{timestep}, {nb_decision} decisions so far. Decision per sec:{dec_per_sec}, timestep per sec:{ts_per_sec}")

        state = readout_state(timestep)
        state_decided = call_peer('http://127.0.0.1:5000',
                                  'decide_on_state',
                                  state=state)

        assert state_decided

        next_action = call_peer('http://127.0.0.1:5000',
                                'sample_next_action')


        decision = call_peer('http://127.0.0.1:5000',
                             'decide_on_action',
                             action=next_action)

        if decision:
            nb_decision += 1
            if False: # timestep >= 3800:

                print(f"Timestep :{timestep}" + "-"*80)
                print(f"Exp.  State: {state}")
                print(f"Exp. Action: {actions[timestep]}")

                print()
                print(f"Next action: {next_action}")
                for k,v in  call_peer('http://127.0.0.1:5000','debug').items():
                    print(f"DBG: {k} : {v}")

                print()
                for i in range(10):
                    print(f"Exp. Action +{i}: {actions[timestep+i]}")

            test_action(next_action, timestep)

        else:
            print("undecided")

run()
exit()


r = call_peer('http://127.0.0.1:5000',
              'acceptable_action',
              action=leader.sample_next_action())

r = call_peer('http://127.0.0.1:5000',
              'decide_on_action',
              action=leader.sample_next_action())

r = call_peer('http://127.0.0.1:5000', 'acceptable_state', state=state)

r = call_peer('http://127.0.0.1:5000', 'decide_on_state', state=state)

print(r)
