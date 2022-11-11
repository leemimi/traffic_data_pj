import pandas as pd
import time
from datetime import timedelta
from multiprocessing import Manager, Process, freeze_support, set_start_method, Pool


def datacount(df, group, rowForDelete):
    index, count = group
    count = len(count)
    need2Delete = 'X'  # 삭제 대상 여부
    print("-" * 30)
    print(f'그룹 {index}')
    if (count < 30):
        targetIndex = df[df['가상OBU_ID'] == index].index
        rowForDelete.extend(targetIndex)
        need2Delete = 'O'
    print(f"삭제 대상 : {need2Delete}, 갯수: {count}")
    print("-" * 30)


if __name__ == '__main__':

    # initialize
    freeze_support()  # multiprocessing 을 위한 빌드업
    dataPath = "D:\\dsrcfile\\month_7\\20210704.csv"  # 원본 자료 링크
    numCores = 6
    manager = Manager()
    rowForDelete = manager.list()  # 모든 프로세스에서 접근 가능한 리스트 (최종으로 삭제할 행의 인덱스를 저장할 리스트)
    jobs = []

    # startTime
    print("처리 시작")
    start = time.process_time()

    # mainFunc
    print("파일 읽기 시작")
    df = pd.read_csv(dataPath, encoding='cp949')
    groups = df.groupby('가상OBU_ID', sort=False)
    # print(df.shape) #현재 데이터프레임의 상태를 파악하기 위한 print
    print("파일 읽기 완료")

    # 실행할 그룹들
    print("멀티 프로세싱을 이용한 파일 처리 진행 중")
    for group in groups:
        p = Process(target=datacount, args=(df, group, rowForDelete))
        jobs.append(p)
        p.start()

    for job in jobs:
        job.join()
    print("멀티 프로세싱을 이용한 파일 처리 진행 중")

    # 멀티프로세싱 이후
    print("행 삭제 진행")
    df = df.drop(rowForDelete)
    # print(df.shape) #현재 데이터프레임의 상태를 파악하기 위한 print

    print("결과물 저장 중")
    # 결과물을 csv로 변환 후 저장
    df.to_csv("C:\\Users\\User\\Desktop\\dropT_20210703.csv", encoding='cp949')

    # endTime
    end = time.process_time()

    print("처리 끝")
    print("Time elapsed: ", end - start)  # seconds
    print("Time elapsed: ", timedelta(seconds=end - start))
