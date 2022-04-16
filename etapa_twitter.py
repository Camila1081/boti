import requests
import os
import pandas as pd


# To set your environment variables in your terminal run the following line:
# export 'BEARER_TOKEN'='<your_bearer_token>'
def run_twitter(linha_top):
    bearer_token = os.environ.get("BEARER_TOKEN")
    project_name = "boti-347200"
    print(linha_top)
    # search_url = "https://api.twitter.com/2/tweets/search/recent"    
    query = f'Boticário {linha_top} lang:pt -is:retweet'
    tweet_fields = "tweet.fields=author_id,conversation_id,created_at,id,in_reply_to_user_id,public_metrics,text,lang"
    user_fields = "expansions=author_id&user.fields=id,name,username,created_at"
    # lingua = "lang%3Apt"
    search_url = "https://api.twitter.com/2/tweets/search/recent?query={}&{}&{}".format(
        query, tweet_fields, user_fields
    )
    # Optional params: start_time,end_time,since_id,until_id,max_results,next_token,
    # expansions,tweet.fields,media.fields,poll.fields,place.fields,user.fields
    # query_params = {'query': '(from:twitterdev -is:retweet) OR #twitterdev','tweet.fields': 'author_id',"max_results" : 100}
    query_params = {"max_results": 100}

    # json_response = connect_to_endpoint(self.search_url, self.query_params)
    def bearer_oauth(r):
        """
        Method required by bearer token authentication.
        """

        r.headers["Authorization"] = f"Bearer {bearer_token}"
        r.headers["User-Agent"] = "v2RecentSearchPython"
        return r

    response = requests.get(search_url, auth=bearer_oauth, params=query_params)

    if response.status_code != 200:
        raise Exception(response.status_code, response.text)

    json_response = response.json()

    data = json_response.get("data")
    users = json_response.get("includes")

    df_text = pd.DataFrame(data[:])
    df_text = pd.DataFrame(df_text, columns=["text", "author_id", "lang"])
    df_text = df_text[df_text.lang == "pt"]
    df_name = pd.DataFrame(users["users"])
    df_name = df_name[["username", "id"]]

    df_bq = df_name.merge(df_text, left_on="id", right_on="author_id")
    df_bq = df_bq.dropna()
    df_bq = df_bq[["username", "text"]]
    print(df_bq[0:49])

    df_bq[0:49].to_gbq(
        destination_table="dados_twitter.tweets_recentes",
        project_id=project_name,
        if_exists="replace",
    )


#if __name__ == "__main__":
 #   run_twitter(linha_top='maquiagem')
