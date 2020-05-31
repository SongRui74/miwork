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

def transcsv(inputpath , feature="videoDuration", output="./raw.csv"):
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
    writer.writerow(["deviceId",feature])
    miss = 0
    number = 0
    with open(inputpath, 'r', encoding='utf-8') as file:
        for line in file.readlines():
            data = json.loads(line)
            number = number + 1
            if feature.__eq__("age") or feature.__eq__("gender"):
                try:
                    deviceId = data['commonStrMap']['deviceId']
                    value = data['commonIntMap'][feature]
                except:
                    deviceId = None
                    value = None
                    miss = miss + 1
            else:
                try:
                    deviceId = data['commonStrMap']['deviceId']
                    value = data['commonStrMap'][feature]
                except:
                    value = None
                    deviceId = None
                    miss = miss + 1
            writer.writerow([deviceId,value])
    csv_file.close()
    file.close()
    print(miss)
    print(number)

def readcsv2(input="./raw.csv",feature='age'):
    reader = pd.read_csv(input, iterator=True, dtype='float')
    loop = True
    chunksize = 6000
    chunks = []
    while loop:
        try:
            chunk = reader.get_chunk(chunksize)
            chunk = chunk.dropna(axis=1)
            chunk = chunk.drop_duplicates(['deviceId'])
            chunks.append(chunk)
        except StopIteration:
            loop = False
            print("Iteration is stopped.")
    df = pd.concat(chunks, ignore_index=True)
    df = pd.DataFrame(df)
    df = df.drop_duplicates(['deviceId']).reset_index(drop=True)
    df[feature].value_counts().to_csv("./result/" + feature + ".csv", sep="\t", encoding="utf8")
    print(df.describe())

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
    inputpath = './rawdata'

    # start = time.clock()
    # readrawdata(inputpath)
    # end = time.clock()
    # print(end - start)

    transcsv_D(inputpath)
    # start = time.clock()
    # readcsv()
    # end = time.clock()
    # print(end - start)
    #
    start = time.clock()
    readcsv2()
    end = time.clock()
    print(end - start)


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
