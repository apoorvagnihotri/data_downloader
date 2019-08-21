# %%
import numpy as np
import json
from collections import defaultdict
import random
import sys
from netsapi.challenge import *

# %%
class Q_Agent():
    
    def __init__(self, environment):

        jpath = sys.argv[2] # getting json path
        try:
            with open(jpath, 'r') as f:
                jfile = json.load(f)
        except json.decoder.JSONDecodeError:
            print ("Hello json is corrupted!!")
            sys.exit()

        p = jfile['tests'][int(sys.argv[1])] # getting the correct test to run
        self.p = p

        #Hyperparameters
        self.env = environment
        self.epsilon = p['epsilon']
        self.div_epsilon = p['div_epsilon']
        self.gamma = p['gamma']
        self.min_epsilon = p['min_epsilon']
        self.action_resolution = p['action_resolution']
        self.Q = defaultdict(lambda : p['defaultQ']) # Q-function
        self.visit_count = defaultdict(lambda : p['defaultVisit']) # number of visits

        # not so much of hyper params
        self.actions = self.actionSpaceCreator(self.action_resolution)
        self.actionspace = range(len(self.actions))
        
    
    def actionSpaceCreator(self, action_resolution):
        u = np.arange(0,1+action_resolution,action_resolution)
        v = np.arange(0,1+self.action_resolution,self.action_resolution)
        x, y = np.meshgrid(u, v)
        xy = np.concatenate((x.reshape(-1,1), y.reshape(-1,1)), axis=1)
        return xy.round(2).tolist()

    def train(self):
        Q = self.Q
        visit_count = self.visit_count
        epsilon = self.epsilon
        min_epsilon = self.min_epsilon
        div_epsilon = self.div_epsilon
        actions = self.actions
        actionspace = self.actionspace
        gamma = self.gamma

        greedy_action = lambda s : max(actionspace, key=lambda a : Q[(s,a)])
        max_q = lambda sp : max([Q[(sp,a)] for a in actionspace])
        
        for _ in range(20): #Do not change
            
            self.env.reset()
            nextstate = self.env.state
            
            while True:
                state = nextstate

                # Epsilon-Greedy Action Selection
                if epsilon > random.random() :
                    action = random.choice(actionspace)
                else :
                    action = greedy_action(state)

                env_action = actions[action]#convert to ITN/IRS
                print('env_action', env_action)
                nextstate, reward, done, _ = self.env.evaluateAction(env_action)

                # Q-learning
                qval = Q[(state,action)] # current q val in Q table
                n = visit_count[(state,action)]
                lr = (1./n)
                if self.p['lr'] == -1:
                    lr = self.p['lr'] # making constant lr

                if done :
                    # changing eps
                    n_eps = 1. * epsilon / div_epsilon # reduce epsilon with episodes
                    if n_eps < min_epsilon:
                        n_eps = min_epsilon
                    epsilon = n_eps

                    qval = qval + lr * (reward - qval)
                    Q[(state,action)] = qval
                    break
                else :
                    qval = qval + lr * (reward + gamma * max_q(nextstate) - qval)
                    Q[(state,action)] = qval
        return Q


    def generate(self):
        best_policy = None
        best_reward = -float('Inf')
        actionspace = self.actionspace
        actions = self.actions
        
        Q_trained = self.train()
        greedy_eval = lambda s : max(actionspace, key=lambda a : Q_trained[(s,a)])
        
        best_policy = {state: list(actions[greedy_eval(state-1)]) for state in range(1,6)}
        best_reward = self.env.evaluatePolicy(best_policy)
        
        print(best_policy, best_reward)
        
        return best_policy, best_reward

# %%
pfil = str(__file__).replace('.py', '')
EvaluateChallengeSubmission(ChallengeSeqDecEnvironment, Q_Agent, f"scores/{pfil}{sys.argv[1]}.csv")