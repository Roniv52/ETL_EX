import datetime

import pandas as pd


def main():
    duration_map = {
        'Season': 1800,
        'min': 60,
        'Seasons': 1800
    }

    df = pd.read_csv('./netflix_titles.csv')
    df = df.dropna()
    df['date_added'] = df['date_added'].str.lstrip()
    df['date_added_datetime'] = pd.to_datetime(df['date_added'], format="%B %d, %Y")
    df = df[df['date_added_datetime'] > "2016-01-01"]
    df['current_date'] = datetime.datetime.now()
    df['number_of_catagories'] = df['listed_in'].str.split(',')
    df['number_of_catagories'] = df['number_of_catagories'].map(len)
    df[['splitted_duration_0', 'splitted_duration_1']] = df['duration'].str.split(" ", expand=True)
    df['duration_in_seconds'] = df['splitted_duration_0'].astype(int) * df['splitted_duration_1'].map(duration_map).astype(int)

    directors_df = df.assign(director=df['director'].str.split(',')).explode('director')
    directors_df = directors_df.dropna(subset=['director'])
    print(directors_df.groupby('director')['duration_in_seconds'].mean())

    countries_df = df.assign(country=df['country'].str.split(',')).explode('country')
    countries_df = countries_df.assign(listed_in=countries_df['listed_in'].str.split(',')).explode('listed_in')
    countries_df = countries_df[countries_df['country'] != ""]
    countries_df['country'] = countries_df['country'].str.lstrip()
    countries_df['listed_in'] = countries_df['listed_in'].str.lstrip()
    print(countries_df.groupby(['country', 'listed_in']).size())

    # print(df.to_string())


if __name__ == '__main__':
    main()


