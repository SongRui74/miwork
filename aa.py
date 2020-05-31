import time
import pandas as pd
import json
import csv

#连续
def readrawdata(inputpath):
    feature = "videoDuration"
    df = pd.DataFrame()
    with open(inputpath, 'r', encoding='utf-8') as file:
        for line in file.readlines():
            datalist = json.loads(line)
            dic = dict()
            try:
                dic[feature] = datalist['featureIntMap'][feature]
            except:
                dic[feature] = None
            data_df = pd.DataFrame(dic, index=["0"], dtype='float')
            df = df.append(data_df, ignore_index = True)
    print(df)
    file.close()

def transcsv(inputpath , feature="videoDuration", output="./raw_2.csv"):
    csv_file = open(output, 'w', encoding='utf-8',newline="")  # 默认newline='\n'
    writer = csv.writer(csv_file)
    writer.writerow([feature])
    miss = 0
    number = 0
    with open(inputpath, 'r', encoding='utf-8') as file:
        for line in file.readlines():
            data = json.loads(line)
            number = number + 1
            try:
                value = data['featureIntMap'][feature]
            except:
                value = None
                miss = miss + 1
            writer.writerow([value])
    csv_file.close()
    file.close()
    print(miss)
    print(number)

def transcsv_D(inputpath , feature="age", output="./raw.csv"):
    csv_file = open(output, 'w', encoding='utf-8',newline="")  # 默认newline='\n'
    writer = csv.writer(csv_file)
    featurelist = ["deviceId", feature]
    writer.writerow(featurelist)
    number = 0
    miss = 0
    with open(inputpath, 'r', encoding='utf-8') as file:
        for line in file.readlines():
            number = number + 1
            if number % 5000 == 0:
                print(str(number) + " lines")
            data = json.loads(line)
            value = list()
            for feature in featurelist:
                try:
                    value.append(data['commonStrMap'][feature])
                except:
                    value.append(None)
                    miss = miss + 1
            writer.writerow(value)
    csv_file.close()
    file.close()
    #原始数据特征缺失值
    print(miss)
    print(number)

def readcsv2(input="./raw.csv",feature='age'):
    reader = pd.read_csv(input, iterator=True)
    loop = True
    chunksize = 6000
    chunks = []
    while loop:
        try:
            chunk = reader.get_chunk(chunksize)
            chunk = chunk.drop_duplicates(['deviceId'])
            chunks.append(chunk)
        except StopIteration:
            loop = False
            print("Iteration is stopped.")
    df = pd.concat(chunks, ignore_index=True)
    df = pd.DataFrame(df)
    df = df.drop_duplicates(['deviceId']).reset_index(drop=True)
    #计算去重后的特征缺失值
    miss = df.isnull().sum().tolist()[1]
    number = df.shape[0]
    ratio = 100 * float(miss) / float(number)
    # print(miss)
    # print(ratio)
    # print(number)
    df[feature].value_counts().to_csv("./result/" + feature + ".csv", sep="\t", encoding="utf-8")

import glob, os
def read_multicsv():
    path = r'F:\PycharmProjects\data'
    file = glob.glob(os.path.join(path, "raw_*.csv"))
    print(file)
    dl = []
    for f in file:
        reader = pd.read_csv(f, iterator=True, dtype='float')
        loop = True
        chunksize = 6000
        chunks = []
        while loop:
            try:
                chunk = reader.get_chunk(chunksize)
                chunks.append(chunk)
            except StopIteration:
                loop = False
        dtemp = pd.concat(chunks, ignore_index=True)
        dl.append(dtemp)
    df = pd.concat(dl)
    df = pd.DataFrame(df.describe())
    df.to_csv("./out.csv", sep="\t")


def readcsv(input="./raw.csv"):
    reader = pd.read_csv(input,iterator=True, dtype='float')
    loop = True
    chunksize = 6000
    chunks = []
    while loop:
        try:
            chunk = reader.get_chunk(chunksize)
            chunks.append(chunk)
        except StopIteration:
            loop = False
            print("Iteration is stopped.")
    df = pd.concat(chunks, ignore_index=True)
    df = pd.DataFrame(df.describe())
    df.to_csv("./out.csv",sep="\t")
    print(df.describe())

if __name__ == '__main__':
    read_multicsv()

    inputpath = './rawdata'

    # start = time.clock()
    # readrawdata(inputpath)
    # end = time.clock()
    # print(end - start)

    # transcsv(inputpath)
    # start = time.clock()
    # readcsv()
    # end = time.clock()
    # print(end - start)
    #
    # transcsv_D(inputpath)
    # start = time.clock()
    # readcsv2()
    # end = time.clock()
    # print(end - start)


#离散
# def readrawdata():
#     dic = {"id":[1,1,2,2,3,4],
#            "age":[12,12,12,12,23,67],
#            "city":["b","b","c","c","f","g"]
#            }
#     df = pd.DataFrame(dic)
#     print(df)
#
#     df2 = df.drop_duplicates(['id']).reset_index(drop = True)
#     print(df2)
#
#     print(max(dic.values()))
