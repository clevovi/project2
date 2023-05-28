
#Purpose of Database

Due to their user base growth, Sparkify needs to deploy their music streaming analytical processes to the cloud. 

Here we have an ETL pipeline that extracts their data from S3 then stages them in Redshift and finally transforms the data into a set of tables for their analytics team. This will help their team analyze which song their users are listening too. 

#Database schema design

The Sparkfy Star Schema is described as songplays as keys with variables users, songs, artists, and time.
