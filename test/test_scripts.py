import pandas as pd
import sqlite3

def load_data():
    """Load CSV files into DataFrames."""
    albums_file = 'path_to_your_file/spotify-albums_data_2023.csv'
    tracks_file = 'path_to_your_file/spotify_tracks_data_2023.csv'

    albums_df = pd.read_csv(albums_file)
    tracks_df = pd.read_csv(tracks_file)
    
    return albums_df, tracks_df

def clean_data(tracks_df):
    """Clean the tracks DataFrame by dropping unnecessary columns."""
    columns_to_drop = ['album_type', 'artist_id', 'artist_0', 'artist_1', 'artist_2', 
                       'artist_3', 'artist_4', 'artist_5', 'artist_6', 'artist_7', 
                       'artist_8', 'artist_9', 'artist_10', 'artist_11']
    
    # Filter columns that are actually present in the DataFrame
    columns_to_drop = [col for col in columns_to_drop if col in albums_df.columns]

    # Drop the columns
    albums_df_cleaned = albums_df.drop(columns=columns_to_drop)

    return albums_df_cleaned

def add_radio_mix(albums_df_cleaned):
    """Add a new column 'radio_mix' based on song duration."""
    albums_df_cleaned['radio_mix'] = albums_df_cleaned['duration_sec'].apply(lambda x: True if x <= 180 else False)
    return albums_df_cleaned

def merge_data(albums_df_cleaned, albums_df):
    """Merge tracks and albums DataFrames on 'track_id'."""
    merged_df = pd.merge(albums_df_cleaned, tracks_file, left_on='track_id', right_on='id', how='inner')
    return merged_df

def filter_data(merged_df):
    """Filter non-explicit songs with track popularity greater than 50%."""
    filtered_df = merged_df[(merged_df['explicit'] == False) & (merged_df['track_popularity'] > 50)]
    return filtered_df

def load_to_db(filtered_df):
    """Load the filtered DataFrame into SQLite database."""
    conn = sqlite3.connect('spotify_data.db')
    filtered_df.to_sql('spotify_tracks', conn, if_exists='replace', index=False)
    conn.close()

def query_top_labels():
    """Query the top 20 labels and their total number of tracks."""
    conn = sqlite3.connect('spotify_data.db')
    query = '''
    SELECT label, COUNT(track_id) as total_tracks
    FROM spotify_tracks
    GROUP BY label
    ORDER BY total_tracks DESC
    LIMIT 20
    '''
    top_labels = pd.read_sql(query, conn)
    conn.close()
    return top_labels

def query_top_tracks():
    """Query the top 25 popular tracks released between 2020 and 2023."""
    conn = sqlite3.connect('spotify_data.db')
    query = '''
    SELECT track_name, release_date, track_popularity
    FROM spotify_tracks
    WHERE release_date BETWEEN '2020-01-01' AND '2023-12-31'
    ORDER BY track_popularity DESC
    LIMIT 25
    '''
    top_tracks = pd.read_sql(query, conn)
    conn.close()
    return top_tracks

def test_data_loading(albums_df, tracks_df):
    """Test the data loading."""
    assert len(albums_df) > 0, "Albums DataFrame is empty!"
    assert len(tracks_df) > 0, "Tracks DataFrame is empty!"
    print("Data loading test passed!")

def test_column_dropping(albums_df_cleaned, columns_to_drop):
    """Test if the correct columns were dropped."""
    for col in columns_to_drop:
        assert col not in albums_df_cleaned.columns, f"Column {col} was not removed!"
    print("Column dropping test passed!")

def test_radio_mix_column(albums_df_cleaned):
    """Test if the 'radio_mix' column is added correctly."""
    assert 'radio_mix' in albums_df_cleaned.columns, "'radio_mix' column not added!"
    assert albums_df_cleaned.loc[albums_df_cleaned['duration_sec'] == 150, 'radio_mix'].iloc[0] == True, "Incorrect 'radio_mix' value for duration_sec=150"
    assert albums_df_cleaned.loc[albums_df_cleaned['duration_sec'] == 200, 'radio_mix'].iloc[0] == False, "Incorrect 'radio_mix' value for duration_sec=200"
    print("'radio_mix' column test passed!")

def test_merge(merged_df):
    """Test if merging was successful."""
    assert len(merged_df) > 0, "Merge result is empty!"
    assert 'track_name' in merged_df.columns, "'track_name' column missing after merge!"
    assert 'album_name' in merged_df.columns, "'album_name' column missing after merge!"
    print("Merge test passed!")

def test_filtering(filtered_df):
    """Test if filtering works correctly."""
    assert len(filtered_df) > 0, "Filtered DataFrame is empty!"
    assert all(filtered_df['explicit'] == False), "Some rows are explicit in filtered DataFrame!"
    assert all(filtered_df['track_popularity'] > 50), "Some rows have track_popularity <= 50 in filtered DataFrame!"
    print("Filtering test passed!")

def test_db_insertion():
    """Test if data is successfully inserted into the database."""
    conn = sqlite3.connect('spotify_data.db')
    query = 'SELECT COUNT(*) FROM spotify_tracks'
    result = pd.read_sql(query, conn)
    conn.close()
    
    assert result.iloc[0, 0] > 0, "Database insertion failed!"
    print("Database insertion test passed!")

def main():
    """Main function to run the complete process."""
    
    # Step 1: Load the data
    albums_df, tracks_df = load_data()
    
    # Run tests for data loading
    test_data_loading(albums_df, tracks_df)
    
    # Step 2: Clean the albums data
    albums_df_cleaned = clean_data(albums_df)
    
    # Run tests for column dropping
    columns_to_drop = ['album_type', 'artist_id', 'artist_0', 'artist_1', 'artist_2', 
                       'artist_3', 'artist_4', 'artist_5', 'artist_6', 'artist_7', 
                       'artist_8', 'artist_9', 'artist_10', 'artist_11']
    test_column_dropping(albums_df_cleaned, columns_to_drop)
    
    # Step 3: Add 'radio_mix' column
    albums_df_cleaned = add_radio_mix(albums_df_cleaned)
    
    # Run tests for 'radio_mix' column
    test_radio_mix_column(albums_df_cleaned)
    
    # Step 4: Merge tracks and albums data
    merged_df = merge_data(albums_df_cleaned, tracks_df)
    
    # Run tests for merging
    test_merge(merged_df)
    
    # Step 5: Filter non-explicit songs with track_popularity > 50
    filtered_df = filter_data(merged_df)
    
    # Run tests for filtering
    test_filtering(filtered_df)
    
    # Step 6: Load data into the database
    load_to_db(filtered_df)
    
    # Run tests for database insertion
    test_db_insertion()
    
    # Step 7: Query top 20 labels
    top_labels = query_top_labels()
    print("Top 20 Labels:")
    print(top_labels)
    
    # Step 8: Query top 25 tracks
    top_tracks = query_top_tracks()
    print("Top 25 Tracks:")
    print(top_tracks)
    
if __name__ == "__main__":
    main()
