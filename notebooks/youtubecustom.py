import pandas as pd

from googleapiclient.errors import HttpError

def get_video_categories(region_code, youtube):
    """Get a list of video categories for a given region code.
    
    Parameters
    ----------
    region_code : str
        The region code to get the video categories for. For example, "GB" for the UK.
    youtube : googleapiclient.discovery.Resource
        The YouTube API client.
    
    Returns
    -------
    pd.DataFrame
        A DataFrame containing the video categories for the given region code.
    
    """

    request = youtube.videoCategories().list(
        part="snippet",  # It's complicated, but this is the part of the API that we want to use. Read more here: https://developers.google.com/youtube/v3/getting-started#part
        regionCode=region_code, # Focus on the UK
        prettyPrint=True
    )
    response = request.execute()
    df = pd.json_normalize(response["items"])

    # I don't really care about `king`, `etag, `chanelId`, so I'll drop them
    df.drop(columns=["kind", "etag", "snippet.channelId"], inplace=True)

    # We can also rename the columns to make them more readable
    df.rename(columns={"snippet.title": "category_title", "snippet.assignable": "assignable"}, inplace=True)

    # Add a column with the region code
    df["region_code"] = region_code

    return df

def get_most_popular_videos(region_code, youtube, video_category_id=None, max_results=50, verbose=False):
    """Get a list of most popular videos for a given region code and video category ID.
    
    Parameters
    ----------
    region_code : str
        The region code to get the video categories for. For example, "GB" for the UK.
    youtube : googleapiclient.discovery.Resource
        The YouTube API client.
    video_category_id : str, optional
        The video category ID to filter the videos by. For example, "1" for Film & Animation.
    max_results : int, optional
        The maximum number of results to return. The default is 20.
    
    Returns
    -------
    pd.DataFrame
        A DataFrame containing the most popular videos for the given region code and video category ID.
    
    """

    try:
        request = youtube.videos().list(
            part="snippet, statistics,contentDetails, topicDetails", 
            chart="mostPopular",
            regionCode=region_code,
            maxResults=max_results,
            videoCategoryId=video_category_id
        )

        response = request.execute()
        df = pd.json_normalize(response["items"])

        # I don't really care about `king`, `etag, `chanelId`, so I'll drop them
        df.drop(columns=["etag", "kind", "snippet.channelId"], inplace=True)

        selected_columns = ['id', 'snippet.publishedAt', 'snippet.channelTitle', 'snippet.localized.title', 
                            'snippet.localized.description', 'contentDetails.duration']

        # Get me all the columns that contain the word "statistics"
        statistics_columns = [col for col in df.columns if "statistics" in col]

        # Append the selected columns to the statistics columns
        selected_columns = selected_columns + statistics_columns

        # copy() is used to avoid SettingWithCopyWarning (https://www.dataquest.io/blog/settingwithcopywarning/)
        df = df[selected_columns].copy() 

        # We can also rename the columns to make them more readable
        df.rename(columns={"snippet.publishedAt": "published_at", 
                        "snippet.channelTitle": "channel_title", 
                        "snippet.localized.title": "title", 
                        "snippet.localized.description": "description", 
                        "contentDetails.duration": "duration",
                        "statistics.viewCount": "view_count",
                        "statistics.likeCount": "like_count", 
                        "statistics.favoriteCount": "favorite_count",
                        "statistics.commentCount": "comment_count"},
                        inplace=True)

        # Add the region code and video category ID to the DataFrame
        df["region_code"] = region_code
        df["video_category_id"] = video_category_id

    except Exception as e:
        if verbose:
            print(f"The query for region_code={region_code} and video_category_id={video_category_id} failed with the following error:\n{e}\n")
        df = pd.DataFrame()

    return df
    

def get_top_level_comments(video_id, youtube_client):
    """Get the top level comments for a given video ID.
    
    Parameters
    ----------
    video_id : str
        The video ID to get the top level comments for.
    youtube_client : googleapiclient.discovery.Resource
        The YouTube API client.
    
    Returns
    -------
    pd.DataFrame
        A DataFrame containing the top level comments for the given video ID.
    
    """
    try:
        # Query just 100 comments at a time
        response = youtube_client.commentThreads()\
            .list(part='id,replies,snippet', videoId=video_id, maxResults=100, textFormat='plainText')\
            .execute()

        df_comments = pd.json_normalize(response["items"])\
            .rename(columns={'id': 'comment_id', 
                            'snippet.topLevelComment.snippet.likeCount': 'like_count',
                            'snippet.topLevelComment.snippet.textDisplay': 'comment_text'})

        selected_cols = ['comment_id', 'like_count', 'comment_text']

        if 'replies.comments' in df_comments.columns:
            selected_cols.append('replies.comments')

        df_comments = df_comments[selected_cols].assign(video_id=video_id)
            
        if 'replies.comments' in df_comments.columns:
            df_comments\
            .assign(number_replies=lambda x: x['replies.comments'].apply(lambda y: len(y) if type(y) == list else 0))\
            .sort_values('number_replies', ascending=False)

        if "nextPageToken" in response:
            next_page_token = response["nextPageToken"]
        else:
            next_page_token = None
    except HttpError as e:
        # Typically, this just means the video has disabled comments
        # Uncomment the following line if you suspect something else is going on and you want to see the error message
        # print(f"An HTTP error {e.resp.status} occurred:\n{e.content}")

        df_comments = pd.DataFrame()
        next_page_token = None


    return df_comments, next_page_token
