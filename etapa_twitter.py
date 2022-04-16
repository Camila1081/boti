import requests
import os
import json
import pandas as pd
from google.cloud import bigquery


# To set your environment variables in your terminal run the following line:
# export 'BEARER_TOKEN'='<your_bearer_token>'
def run_twitter():
    bearer_token = os.environ.get("BEARER_TOKEN")
    project_name='boti-347200'
    #search_url = "https://api.twitter.com/2/tweets/search/recent"
    query = 'Boticário OR Maquiagem'
    tweet_fields = "tweet.fields=author_id,conversation_id,created_at,id,in_reply_to_user_id,public_metrics,text"
    user_fields = "expansions=author_id&user.fields=id,name,username,created_at"
    max_results ="max_results=50"
    lingua = " lang%3Apt"
    #search_url= "https://api.twitter.com/2/tweets/search/recent?query=Botic%C3%A1rio&src=typed_query"
    search_url = "https://api.twitter.com/2/tweets/search/recent?query={}{}&{}&{}".format(
            query, lingua,tweet_fields,user_fields
        )
    # Optional params: start_time,end_time,since_id,until_id,max_results,next_token,
    # expansions,tweet.fields,media.fields,poll.fields,place.fields,user.fields
    #query_params = {'query': '(from:twitterdev -is:retweet) OR #twitterdev','tweet.fields': 'author_id',"max_results" : 100}
    query_params = {"max_results":100}

    #json_response = connect_to_endpoint(self.search_url, self.query_params)
    def bearer_oauth(r):
        """
        Method required by bearer token authentication.
        """

        r.headers["Authorization"] = f"Bearer {bearer_token}"
        r.headers["User-Agent"] = "v2RecentSearchPython"
        return r

    response = requests.get(search_url, auth=bearer_oauth,params=query_params)
    
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    
    json_response = response.json()

    
    data=json_response.get("data")
    meta=json_response.get("meta")
    users=json_response.get("includes")

    df_text=pd.DataFrame(data[:])
    df_text=pd.DataFrame(df_text,columns=['text'])
    df_name=pd.DataFrame(users['users'])
    df_name=pd.DataFrame(df_name['username'],columns=['username'])
    df_name.reset_index()
    df_text.reset_index()
    
    df_bq=pd.concat([df_name,df_text],axis=1)
    df_bq.reset_index()
    df_bq=df_bq.dropna()
    print(df_bq)

    df_bq[0:49].to_gbq(destination_table = 'dados_twitter.twits_recentes', project_id=project_name,if_exists='replace' )






if __name__ == "__main__":
    run_twitter()