import argparse
import math
import pickle
import numpy as np

from starter_code.computers import *

# Load the pickle files
actions = pickle.load(open("data/actions.pickle", "rb"))
states = pickle.load(open("data/states.pickle", "rb"))


def readout_state(timestep):
    return states[timestep]


def execute_action(action,timestep):
    print(timestep, action)
    print(actions[timestep])
    for k in action.keys():
        assert(action[k] == actions[timestep][k])


def allocate_flight_computers(arguments, timestep):
    flight_computers = []
    n_fc = arguments.flight_computers
    n_correct_fc = math.ceil(arguments.correct_fraction * n_fc)
    n_incorrect_fc = n_fc - n_correct_fc
    state = readout_state(timestep)
    for _ in range(n_correct_fc):
        flight_computers.append(FlightComputer(state))
    for _ in range(n_incorrect_fc):
        flight_computers.append(allocate_random_flight_computer(state))
    # Add the peers for the consensus protocol
    for fc in flight_computers:
        for peer in flight_computers:
            if fc != peer:
                fc.add_peer(peer)

    return flight_computers



def select_leader():
    leader_index = np.random.randint(0, len(flight_computers))

    return flight_computers[leader_index]


def next_action(state):
    leader = select_leader()
    state_decided = leader.decide_on_state(state)
    if not state_decided:
        return None
    action = leader.sample_next_action()
    action_decided = leader.decide_on_action(action)
    if action_decided:
        return action

    return None


def commandline_args():
    # Argument parsing
    parser = argparse.ArgumentParser()
    parser.add_argument("--correct-fraction", type=float, default=1.0, help="Fraction of correct flight computers (default 1.0).")
    parser.add_argument("--flight-computers", type=int, default=3, help="Number of flight computers (default: 3).")
    arguments, _ = parser.parse_known_args()
    return arguments


if __name__ == "__main__":

    timestep = 0
    # Connect with Kerbal Space Program
    flight_computers = allocate_flight_computers(commandline_args(), timestep)
    timestep += 1

    complete = False
    try:
        while not complete:
            state = readout_state(timestep)
            leader = select_leader()
            state_decided = leader.decide_on_state(state)

            if state_decided:

                action = leader.sample_next_action()
                if action is not None:
                    if leader.decide_on_action(action):
                        execute_action(action, timestep)
                        timestep += 1
                else:
                    complete = True

    except Exception as e:
        print(e)

    if complete:
        print(f"Success after {timestep} timesteps")
    else:
        print("Fail!")
