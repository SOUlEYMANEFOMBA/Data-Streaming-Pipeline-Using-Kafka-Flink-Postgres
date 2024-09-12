from Dags.tasks.stream_data import StreamingDataTask
from Dags.tasks.doawload_data import DoawloadDataTask
from Dags.tasks.format_data import FormatDataTask
def main():
    doawloaddata = DoawloadDataTask('https://randomuser.me/api/')
    res=doawloaddata.get_data()
    formatData =FormatDataTask()
    result =formatData.format_data(res)
    print(result)
    

if __name__=="__main__":
    main()