from Dags.tasks.doawload_data import DoawloadDataTask
from Dags.tasks.format_data import FormatDataTask
from Dags.tasks.stream_data import StreamingDataTask

def main():
    doawloaddata = DoawloadDataTask('https://randomuser.me/api/')
    res=doawloaddata.get_data()
    formatData =FormatDataTask()
    result =formatData.format_data(res)
    print(result)
    stream=StreamingDataTask()
    stream.streaming_data(result)

if __name__=="__main__":
    main()