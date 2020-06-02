import time
import pandas as pd
import json
import csv

#连续,速度慢
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
def transcsv_D(inputpath , output="./raw.csv"):
    csv_file = open(output, 'w', encoding='utf-8',newline="")  # 默认newline='\n'
    writer = csv.writer(csv_file)
    featurelist = ["itemId", "videoCategory", "sourceTags", "videoSize", "nlpCategory", "videoSubCategory", "videoWidth", "videoHeight"]
    #featurelist = ["deviceId", "age","gender","country","province","city","hourOfDay","dayOfWeek","dayOfMonth","minute","model"]
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
                    value.append(data['featureStrMap'][feature])
                    #value.append(data['commonStrMap'][feature])
                except:
                    value.append(None)
                    #miss = miss + 1
            writer.writerow(value)
    csv_file.close()
    file.close()
    #原始数据特征缺失值
    #print(miss)
    #print(number)
def readcsv2(input="./raw.csv"):
    reader = pd.read_csv(input, iterator=True)
    loop = True
    chunksize = 6000
    chunks = []
    while loop:
        try:
            chunk = reader.get_chunk(chunksize)
            #chunk = chunk.drop_duplicates(['deviceId'])
            chunk = chunk.drop_duplicates(['itemId'])
            chunks.append(chunk)
        except StopIteration:
            loop = False
            print("Iteration is stopped.")
    df = pd.concat(chunks, ignore_index=True)
    df = pd.DataFrame(df)
    df = df.drop_duplicates(['itemId']).reset_index(drop=True)
    #df = df.drop_duplicates(['deviceId']).reset_index(drop=True)
    #计算去重后的特征缺失值
    miss = df.isnull().sum().tolist()[1]
    number = df.shape[0]
    ratio = 100 * float(miss) / float(number)
    # print(miss)
    # print(ratio)
    # print(number)
    print(df)
    featurelist = ["itemId", "videoCategory", "sourceTags", "videoSize", "nlpCategory", "videoSubCategory",
                   "videoWidth", "videoHeight"]
    #featurelist = ["deviceId", "age","gender","country","province","city","hourOfDay","dayOfWeek","dayOfMonth","minute","model"]
    for feature in featurelist[1:]:
        df[feature].value_counts(normalize=True,dropna=False).to_csv("./result/" + feature + ".csv", sep="\t", encoding="utf-8")

import glob, os
def read_multicsv():
    path = r'F:\PycharmProjects\data'
    file = glob.glob(os.path.join(path, "raw_*.csv"))
    print(file)
    dl = []
    for f in file:
        reader = pd.read_csv(f, iterator=True)
        loop = True
        chunksize = 1000000
        chunks = []
        i = 0
        while loop:
            try:
                chunk = reader.get_chunk(chunksize)
                chunks.append(chunk)
                i = i + 1
                print(str(i) + "th chunk")
            except StopIteration:
                loop = False
                print("Iteration is stopped.")
        dtemp = pd.concat(chunks, ignore_index=True)
        dl.append(dtemp)
    df = pd.concat(dl)
    df = pd.DataFrame(df.describe(percentiles=[.1, .2, .3, .4, .5, .6, .7, .8, .9]))
    df.to_csv("./out.csv", sep="\t")

def transcsv(inputpath , output="./raw.csv"):
    csv_file = open(output, 'w', encoding='utf-8',newline="")  # 默认newline='\n'
    featurelist=["videoDuration","item12Expose","item24Expose","item72Expose","item12Skip","item24Skip","item72Skip","item12Duration","item24Duration","item72Duration","item12View","item24View","item72View","item12Click","item24Click","item72Click","commentInterval","titleLen"]
    writer = csv.writer(csv_file)
    writer.writerow(featurelist)
    miss = 0
    number = 0
    with open(inputpath, 'r', encoding='utf-8') as file:
        for line in file.readlines():
            number = number + 1
            if number % 5000 == 0:
                print(str(number) + " lines")
            data = json.loads(line)
            value = list()
            for feature in featurelist:
                try:
                    value.append(data['featureIntMap'][feature])
                except:
                    value.append(None)
                    #miss = miss + 1
            writer.writerow(value)
    csv_file.close()
    file.close()
    #print(miss)
    print(number)

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
    #read_multicsv()
    inputpath = '../rawdata'

    # transcsv(inputpath)
    # readcsv()

    transcsv_D(inputpath)
    readcsv2()

