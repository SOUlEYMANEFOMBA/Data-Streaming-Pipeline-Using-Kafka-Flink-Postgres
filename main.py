from Dags.tasks.stream_data import StreamingDataTask
def main():
    streaming = StreamingDataTask('https://randomuser.me/api/')
    streaming.streaming_data()
    

if __name__=="__main__":
    main()