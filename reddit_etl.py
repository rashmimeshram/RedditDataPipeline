import praw
from time import sleep
from datetime import datetime, timezone
import pandas as pd


def run_reddit_etl():
    reddit = praw.Reddit(
        client_id="oZYTQfB8YhwOM9kbXsOO5A",
        client_secret="orWK0L5C73KEg4vnj1-2MDnqKvJy6g",
        user_agent="project_etl_pipeline/rashmitmeshram"
    )

    df = []
    submission = reddit.subreddit("india")
    for post in submission.top(limit=25, time_filter="week"):
        post.dt = datetime.fromtimestamp(post.created_utc).replace(tzinfo=timezone.utc).astimezone(tz=None)
        df.append([post.id, post.title, post.selftext, post.dt, post.url, post.is_self])
    df = pd.DataFrame(df, columns=['id', 'title', 'text', 'date', 'url', 'is-self'])
    df.to_csv("Top_25_Post_of_INDIA.csv")

# submissions = reddit.subreddit('india').stream.submissions()
# for sub in submissions:
#     print('id: ', sub.id)
#     print('title: ', sub.title)
#     print('text: ', sub.selftext)
#     dt = datetime.fromtimestamp(sub.created_utc).replace(tzinfo=timezone.utc).astimezone(tz=None)
#     print('created: ', dt)
#     print('url: ', sub.url)
#     print('is-self: ', sub.is_self)
#     print()
#     sleep(1)
