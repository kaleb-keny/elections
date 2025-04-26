import web3 as W3
import json
from web3.utils import get_abi_output_types
import yaml 
from funcy import chunks
import pandas as pd

with open("conf.yaml", 'r') as stream:
    conf = yaml.load(stream, Loader=yaml.FullLoader)

def get_w3(chain):
    return W3.Web3(W3.HTTPProvider(conf["rpc"][chain],request_kwargs={'timeout': 120}))

def get_contract(chain,name):
    w3       = get_w3(chain)
    return w3.eth.contract(address=conf[name]["address"],abi=conf[name]["abi"])
    
def multicall(contract,functionName,args,chain):
    multicall = get_contract(chain,'multicall')
    decoder   = contract.get_function_by_name(fn_name=functionName)
    decoder   = get_abi_output_types(decoder.abi)        
    w3        = get_w3(chain)
    call_data = list()
    for arg in args:
        if hasattr(arg,"__iter__"):
            call_data.append((contract.address,False,0,contract.encode_abi(functionName,args=arg)))
        else:
            call_data.append((contract.address,False,0,contract.encode_abi(functionName,args=[arg])))
    results = list()
    for call_data_chunk in chunks(1_000,call_data):
        results.extend(multicall.functions.aggregate3Value(call_data_chunk).call())    
    results = [w3.codec.decode(decoder, result[1]) if result[0] else [] for result in results ]        
    return results

def get_account_eoa_map(chain,accounts):
    contract = get_contract(chain,"account")    
    eoas     = multicall(contract,'ownerOf',args=accounts, chain=chain)
    return {account:eoa[0] for account, eoa in zip(accounts,eoas)}

def get_accounts(chain):
    contract = get_contract(chain,"account")
    supply   = contract.functions.totalSupply().call()
    accounts = multicall(contract,'tokenByIndex',args=list(range(supply)),chain=chain)
    return [account[0] for account in accounts]
    
def get_eoa_balances_in_420(chain):
    accounts   = get_accounts(chain)
    snx        = conf["snx"][chain]
    args       = [(account, 8, snx) for account in accounts]
    contract   = get_contract(chain,'core')
    positions  = multicall(contract,'getPosition',args=args, chain=chain)
    df            = pd.DataFrame(positions,columns=["collateral","collateralValue","debt","cratio"])
    df["account"] = accounts
    eoa_map       = get_account_eoa_map(chain=chain, accounts=accounts)
    df["eoa"]     = df["account"].map(eoa_map)
    return df

def update_420_stakers_all_chains():
    df = pd.DataFrame([])
    for chain in [1,10]:
        df = pd.concat([df,get_eoa_balances_in_420(chain)],axis=0)    
    df = df[df["collateral"]>0].copy()
    df["collateral"] = df["collateral"]/1e18
    grouped_df = df.groupby("eoa")["collateral"].sum().reset_index()
    wrapped    = {"symbol":"SNX", "addresses": grouped_df.set_index("eoa")["collateral"].to_dict()}
    with open("election_output.json", "w") as f:
        json.dump(wrapped, f, separators=(",", ":"))
    print("done")

    
#%%
if __name__ == "__main__":
    update_420_stakers_all_chains()
