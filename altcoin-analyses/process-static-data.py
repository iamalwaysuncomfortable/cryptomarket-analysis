import pandas as pd
import webdata

#Determination of Derivative or Unique Blockchain Status
ico_data = webdata.get_ico_data()
categorical_data, replacement_data = webdata.get_static_data()
for token in ico_data.keys():
    coin_data = categorical_data[categorical_data.index.str.match(token)]
    if coin_data.empty is False:
        coin_symbol = coin_data.index[0]
        categorical_data.set_value(coin_symbol, "launch_date", ico_data[token]["launch_date"])
        categorical_data.set_value(coin_symbol, "main_claim", ico_data[token]["main_claim"])
    else:
        row = {}
        row[token] = {"primary_token_use_case": "", "platform_focus": "", "category": "", "guiding_org_type": "",
                      "organization_name": "", "blockchain_node_centralization": "", "smart_contracts": "",
                      "supported_smartcontract_languages": "", "launch_date": ico_data[token]["launch_date"],
                      "main_claim": ico_data[token]["main_claim"]}
        row = pd.DataFrame(row.values(), index=row.keys())
        categorical_data = categorical_data.append(row)

categorical_data.to_csv("test4.csv", encoding="utf-8")


