
import os
import subprocess
import argparse

from pyspark.sql.types import BinaryType
from pyspark.sql import functions as F


def download_stackoverflow_data(url="s3://datasets-documentation/stackoverflow/parquet", year="2008", destination="./stackoverflow_data"):

    # Check if path already exists and is a directory
    if os.path.exists(destination) and os.path.isdir(destination):
        print(f"Data exists already, delete the folder {destination} to re-download")
        return
    
    os.makedirs(destination, exist_ok=True)

    print("=" * 60)
    print("Step 1: Downloading data from S3")
    print("=" * 60)
    
    table_urls = {
    "posts": f"{url}/posts/{year}.parquet"
    , "users": f"{url}/users.parquet"
    , "comments": f"{url}/comments/{year}.parquet"
    , "votes": f"{url}/votes/{year}.parquet"
    , "badges": f"{url}/badges.parquet"
    , "posthistory": f"{url}/posthistory/{year}.parquet"
    , "postlinks": f"{url}/postlinks.parquet"
    } 

    if year == '*':
        table_urls = {
            "posts": f"{url}/posts/"
            , "users": f"{url}/users.parquet"
            , "comments": f"{url}/comments/"
            , "votes": f"{url}/votes/"
            , "badges": f"{url}/badges.parquet"
            , "posthistory": f"{url}/posthistory/"
            , "postlinks": f"{url}/postlinks.parquet"
            } 
        for table, s3_path in table_urls.items():
            table_dir = os.path.join(destination, table)
            os.makedirs(table_dir, exist_ok=True)
                
            print(f"\nDownloading {table}...")
            cmd = f"aws s3 sync {s3_path} {table_dir}/ --no-sign-request"
            
            try:
                subprocess.run(cmd, shell=True, check=True)
                print(f"✓ Downloaded: {table}")
            except subprocess.CalledProcessError as e:
                print(f"✗ Error downloading {table}: {e}")

            return

    

    for table, s3_path in table_urls.items():
        table_dir = os.path.join(destination, table)
        os.makedirs(table_dir, exist_ok=True)
            
        print(f"\nDownloading {table}...")
        cmd = f"aws s3 cp {s3_path} {table_dir}/ --no-sign-request"
        
        try:
            subprocess.run(cmd, shell=True, check=True)
            print(f"✓ Downloaded: {table}")
        except subprocess.CalledProcessError as e:
            print(f"✗ Error downloading {table}: {e}")


def create_bronze_tables(spark):
    
    # Create the database
    spark.sql("CREATE SCHEMA IF NOT EXISTS stackoverflow")
    print("✓ Created database: stackoverflow")
    
    spark.sql("USE stackoverflow")
    
    # ================================================================
    # POSTS TABLE
    # ================================================================
    spark.sql("DROP TABLE IF EXISTS stackoverflow.posts")
    spark.sql("""
    CREATE TABLE IF NOT EXISTS stackoverflow.posts (
        Id INT,
        PostTypeId STRING,
        AcceptedAnswerId INT,
        CreationDate TIMESTAMP,
        Score INT,
        ViewCount INT,
        Body STRING,
        OwnerUserId INT,
        OwnerDisplayName STRING,
        LastEditorUserId INT,
        LastEditorDisplayName STRING,
        LastEditDate TIMESTAMP,
        LastActivityDate TIMESTAMP,
        Title STRING,
        Tags STRING,
        AnswerCount SMALLINT,
        CommentCount SMALLINT,
        FavoriteCount SMALLINT,
        ContentLicense STRING,
        ParentId BINARY,
        CommunityOwnedDate TIMESTAMP,
        ClosedDate TIMESTAMP
    )
    USING PARQUET
    PARTITIONED BY (year(CreationDate))
    """)
    print("✓ Created table: stackoverflow.posts")
    
    # ================================================================
    # USERS TABLE
    # ================================================================
    spark.sql("DROP TABLE IF EXISTS stackoverflow.users")
    spark.sql("""
    CREATE TABLE IF NOT EXISTS stackoverflow.users (
        Id INT,
        Reputation STRING,
        CreationDate TIMESTAMP,
        DisplayName STRING,
        LastAccessDate TIMESTAMP,
        AboutMe STRING,
        Views INT,
        UpVotes INT,
        DownVotes INT,
        WebsiteUrl STRING,
        Location STRING,
        AccountId INT
    )
    USING PARQUET
    PARTITIONED BY (year(CreationDate))
    """)
    print("✓ Created table: stackoverflow.users")
    
    # ================================================================
    # VOTES TABLE
    # ================================================================
    spark.sql("DROP TABLE IF EXISTS stackoverflow.votes")
    spark.sql("""
    CREATE TABLE IF NOT EXISTS stackoverflow.votes (
        Id INT,
        PostId INT,
        VoteTypeId SMALLINT,
        CreationDate TIMESTAMP,
        UserId INT,
        BountyAmount INT
    )
    USING PARQUET
    PARTITIONED BY (year(CreationDate))
    """)
    print("✓ Created table: stackoverflow.votes")
    
    # ================================================================
    # COMMENTS TABLE
    # ================================================================
    spark.sql("DROP TABLE IF EXISTS stackoverflow.comments")
    spark.sql("""
    CREATE TABLE IF NOT EXISTS stackoverflow.comments (
        Id INT,
        PostId INT,
        Score SMALLINT,
        Text STRING,
        CreationDate TIMESTAMP,
        UserId INT,
        UserDisplayName STRING
    )
    USING PARQUET
    PARTITIONED BY (year(CreationDate))
    """)
    print("✓ Created table: stackoverflow.comments")
    
    # ================================================================
    # BADGES TABLE
    # ================================================================
    spark.sql("DROP TABLE IF EXISTS stackoverflow.badges")
    spark.sql("""
    CREATE TABLE IF NOT EXISTS stackoverflow.badges (
        Id INT,
        UserId INT,
        Name STRING,
        Date TIMESTAMP,
        Class TINYINT,
        TagBased BIGINT
    )
    USING PARQUET
    PARTITIONED BY (year(Date))
    """)
    print("✓ Created table: stackoverflow.badges")
    
    # ================================================================
    # POSTLINKS TABLE
    # ================================================================
    spark.sql("DROP TABLE IF EXISTS stackoverflow.postlinks")
    spark.sql("""
    CREATE TABLE IF NOT EXISTS stackoverflow.postlinks (
        Id BIGINT,
        CreationDate TIMESTAMP,
        PostId INT,
        RelatedPostId INT,
        LinkTypeId TINYINT
    )
    USING PARQUET
    PARTITIONED BY (year(CreationDate))
    """)
    print("✓ Created table: stackoverflow.postlinks")
    
    # ================================================================
    # POSTHISTORY TABLE
    # ================================================================
    spark.sql("DROP TABLE IF EXISTS stackoverflow.posthistory")
    spark.sql("""
    CREATE TABLE IF NOT EXISTS stackoverflow.posthistory (
        Id BIGINT,
        PostHistoryTypeId INT,
        PostId INT,
        RevisionGUID STRING,
        CreationDate STRING,
        UserId INT,
        Text STRING,
        ContentLicense STRING,
        Comment STRING,
        UserDisplayName STRING
    )
    USING PARQUET
    """)
    print("✓ Created table: stackoverflow.posthistory")
    
    print("\n✓ All Stack Overflow tables created successfully!")


def load_data_bronze_table(spark, data_path="./stackoverflow_data", tables=['posts', 'users', 'comments', 'votes', 'posthistory']):
    spark.conf.set("spark.sql.legacy.parquet.nanosAsLong", "true")
    spark.sql("use stackoverflow")

    for table in tables:
        
        df = spark.sql(f"select * from parquet.`{data_path}/{table}/`")
        binary_cols = [field.name for field in df.schema.fields if type(field.dataType) == BinaryType]
        for binary_col in binary_cols:
            df = df.withColumn(binary_col, df[binary_col].cast('string'))
        if table == 'posthistory':
            df = df.withColumn(
                "CreationDate",
                F.from_unixtime(F.col("CreationDate") / 1000000000)
            )

        df.write.mode("overwrite").saveAsTable(f"stackoverflow.{table}")
        print(f"✓ Loaded data into: stackoverflow.{table}")

def create_enum_tables(spark):
    post_type_data = [
        (1, "Question"),
        (2, "Answer"),
        (3, "Wiki"),
        (4, "TagWikiExcerpt"),
        (5, "TagWiki"),
        (6, "ModeratorNomination"),
        (7, "WikiPlaceholder"),
        (8, "PrivilegeWiki")
    ]
    post_type_df = spark.createDataFrame(post_type_data, ['id', 'postType'])
    post_type_df.write.mode("overwrite").saveAsTable("stackoverflow.post_type")
    print("✓ Loaded data into: stackoverflow.post_type")

    post_history_type_data = [
        (1, "Initial Title"),
        (2, "Initial Body"),
        (3, "Initial Tags"),
        (4, "Edit Title"),
        (5, "Edit Body"),
        (6, "Edit Tags"),
        (7, "Rollback Title"),
        (8, "Rollback Body"),
        (9, "Rollback Tags"),
        (10, "Post Closed"),
        (11, "Post Reopened"),
        (12, "Post Deleted"),
        (13, "Post Undeleted"),
        (14, "Post Locked"),
        (15, "Post Unlocked"),
        (16, "Community Owned"),
        (17, "Post Migrated"),
        (18, "Question Merged"),
        (19, "Question Protected"),
        (20, "Question Unprotected"),
        (21, "Post Disassociated"),
        (22, "Question Unmerged"),
        (23, "Unknown dev related event"),
        (24, "Suggested Edit Applied"),
        (25, "Post Tweeted"),
        (26, "Vote nullification by dev"),
        (27, "Post unmigrated/hidden moderator migration"),
        (28, "Unknown suggestion event"),
        (29, "Unknown moderator event"),
        (30, "Unknown event"),
        (31, "Comment discussion moved to chat"),
        (33, "Post notice added"),
        (34, "Post notice removed"),
        (35, "Post migrated away"),
        (36, "Post migrated here"),
        (37, "Post merge source"),
        (38, "Post merge destination"),
        (50, "Bumped by Community User"),
        (52, "Question became hot network question"),
        (53, "Question removed from hot network questions by moderator"),
        (66, "Created from Ask Wizard")
    ]

    post_history_type_df = spark.createDataFrame(post_history_type_data, ['id', 'history_type']) 
    post_history_type_df.write.mode("overwrite").saveAsTable("stackoverflow.post_history_type")
    print("✓ Loaded data into: stackoverflow.post_history_type")
    

def run(spark, url="s3://datasets-documentation/stackoverflow/parquet", year="2008", destination="./stackoverflow_data"):
    download_stackoverflow_data(url, year, destination)
    create_bronze_tables(spark)
    load_data_bronze_table(spark, destination)
    create_enum_tables(spark)


def run_dbx(spark, url="s3a://datasets-documentation/stackoverflow/parquet", year="2008", destination="./stackoverflow_data"):
    
    spark.sql("create database if not exists stackoverflow")
    create_enum_tables(spark)

    post_history_url = "https://huggingface.co/datasets/josephmachado/post_history_sample/resolve/main/part-00000-5f1f8f07-8d7c-4c3c-af29-1b4376c3293f-c000.snappy.parquet"
    print("PostHistory: downloading data from {post_history_url}")
    response = requests.get(post_history_url)
    df_pandas = pd.read_parquet(BytesIO(response.content))
    spark.createDataFrame(df_pandas).write.mode("overwrite").saveAsTable("stackoverflow.posthistory")


    if year == '*':
        table_urls = {
            "posts": f"{url}/posts/"
            , "users": f"{url}/users.parquet"
            , "comments": f"{url}/comments/"
            , "votes": f"{url}/votes/"
            , "badges": f"{url}/badges.parquet"
            , "postlinks": f"{url}/postlinks.parquet"
        } 
        for table, table_url in table_urls.items():
            print(f'Loading data from {table_url} and saving it to stackoverflow.{table}')
            df = spark.read.parquet(f"{table_url}")
            binary_cols = [field.name for field in df.schema.fields if type(field.dataType) == BinaryType]
            for binary_col in binary_cols:
                df = df.withColumn(binary_col, df[binary_col].cast('string'))
            df.write.mode("overwrite").saveAsTable(f"stackoverflow.{table}")
        return 

    table_urls = {
    "posts": f"{url}/posts/{year}.parquet"
    , "users": f"{url}/users.parquet"
    , "comments": f"{url}/comments/{year}.parquet"
    , "votes": f"{url}/votes/{year}.parquet"
    , "badges": f"{url}/badges.parquet"
    , "postlinks": f"{url}/postlinks.parquet"
    } 

    
    for table, table_url in table_urls.items():
        print(f'Loading data from {table_url} and saving it to stackoverflow.{table}')
        df = spark.read.parquet(f"{table_url}")
        binary_cols = [field.name for field in df.schema.fields if type(field.dataType) == BinaryType]
        for binary_col in binary_cols:
            df = df.withColumn(binary_col, df[binary_col].cast('string'))
        df.write.mode("overwrite").saveAsTable(f"stackoverflow.{table}")

        

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Download StackOverflow Data")
    parser.add_argument(
            "--url",
            type=str,
            default="s3://datasets-documentation/stackoverflow/parquet",
            help="Data Download URL",
        )
    parser.add_argument(
            "--year",
            type=str,
            default="2008",
            help="Year of data to download, use * for all",
        )
    parser.add_argument(
            "--destination",
            type=str,
            default="./stackoverflow_data",
            help="Output folder path",
        )
    
    args = parser.parse_args()
    run(spark, args.url, args.year, args.destination)
    

