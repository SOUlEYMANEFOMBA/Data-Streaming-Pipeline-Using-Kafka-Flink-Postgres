from Dags.tasks.doawload_data import DoawloadDataTask
from Dags.tasks.format_data import FormatDataTask
from Dags.tasks.stream_data import StreamingDataTask

def main():
    doawloaddata = DoawloadDataTask('https://randomuser.me/api/')
    formatData =FormatDataTask()
    stream=StreamingDataTask()
    stream.streaming_data(doawloaddata,formatData)

if __name__=="__main__":
    main()