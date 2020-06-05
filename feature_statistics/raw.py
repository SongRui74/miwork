# -*- coding:utf-8 -*-
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
def transcsv_D(inputpath , output="./result/raw.csv"):
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
def readcsv2(input="./result/raw.csv"):
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



def transcsv(inputpath , output="./result/raw.csv"):
    csv_file = open(output, 'w', encoding='utf-8',newline="")  # 默认newline='\n'
    featurelist=["videoDuration","item12Expose","item24Expose","item72Expose","item12Skip","item24Skip","item72Skip","item12Duration","item24Duration","item72Duration","item12View","item24View","item72View","item12Click","item24Click","item72Click","commentInterval","titleLen"]
    writer = csv.writer(csv_file)
    writer.writerow(featurelist)
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
            writer.writerow(value)
    csv_file.close()
    file.close()
    print(number)
def readcsv(input):
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
    df.to_csv("./result/out.csv",sep="\t")
    print(df.describe())

def rawtocsv(inputpath , output):
    infolist = ["userId","itemId","path","duration","action"]
    featureIntMap=["videoDuration","item12Expose","item24Expose","item72Expose","item12Skip","item24Skip","item72Skip","item12Duration","item24Duration","item72Duration","item12View","item24View","item72View","item12Click","item24Click","item72Click","commentInterval","titleLen"]
    commonStrMap = ["country","gender","phone_channel_code","city","phone_is_international","phone_brand","deviceId","current_province","current_country","minute","miui_big_version","dayOfWeek","province","phone_telecom","dayOfMonth","current_city","hourOfDay","current_district","model","miui_release","screenResolution","age","miui_version"]
    featureDoubleMap = ["item72VideoCTR","item24VideoCTR","item12VideoCTR"]
    featureStrMap = ["videoWidth","itemId","videoCategory","sourceTags","boardId","authorId","videoSize","nlpCategory","videoHeight","opRecomType","videoSubCategory"]
    featurelist = infolist+featureIntMap+commonStrMap+featureDoubleMap+featureStrMap
    csv_file = open(output, 'w', encoding='utf-8', newline="")  # 默认newline='\n'
    writer = csv.writer(csv_file)
    writer.writerow(featurelist)
    number = 0
    with open(inputpath, 'r', encoding='utf-8') as file:
        for line in file.readlines():
            number = number + 1
            if number % 5000 == 0:
                print(str(number) + " lines")
            data = json.loads(line)
            value = list()
            for feature in infolist:
                try:
                    value.append(data[feature])
                except:
                    value.append(None)
            for feature in featureIntMap:
                try:
                    value.append(data['featureIntMap'][feature])
                except:
                    value.append(None)
            for feature in commonStrMap:
                try:
                    value.append(data['commonStrMap'][feature])
                except:
                    value.append(None)
            for feature in featureDoubleMap:
                try:
                    value.append(data['featureDoubleMap'][feature])
                except:
                    value.append(None)
            for feature in featureStrMap:
                try:
                    value.append(data['featureStrMap'][feature])
                except:
                    value.append(None)
            writer.writerow(value)
    csv_file.close()
    file.close()
import glob, os
def read_multicsv(output):
    path = r'F:\PycharmProjects\data\result'
    file = glob.glob(os.path.join(path, "raw*.csv"))
   # print(file)
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

    #统计条件筛选后的commentInterval
    result = df[(df['path']==19) & ( (df['action'] == 'CLICK') | (df['action'] == 'EXPOSE'))]
    pd.DataFrame(result['commentInterval'].describe(percentiles=[.1, .2, .3, .4, .5, .6, .7, .8, .9])).to_csv(output, mode="a", sep="\t")

    # 统计未筛选的
    featureIntMap = ["videoDuration", "item12Expose", "item24Expose", "item72Expose", "item12Skip", "item24Skip",
                     "item72Skip", "item12Duration", "item24Duration", "item72Duration", "item12View", "item24View",
                     "item72View", "item12Click", "item24Click", "item72Click", "commentInterval", "titleLen"]
    for feature in featureIntMap:
        result = pd.DataFrame(df[feature].describe(percentiles=[.1, .2, .3, .4, .5, .6, .7, .8, .9]))
        result.to_csv(output, mode="a", sep="\t")

    featureDoubleMap = ["item72VideoCTR", "item24VideoCTR", "item12VideoCTR"]
    for feature in featureDoubleMap:
        result = pd.DataFrame(df[feature].describe(percentiles=[.1, .2, .3, .4, .5, .6, .7, .8, .9]))
        result.to_csv(output, mode="a", sep="\t")

    commonStrMap = ["country","gender","phone_channel_code","city","phone_is_international","phone_brand","deviceId","current_province","current_country","minute","miui_big_version","dayOfWeek","province","phone_telecom","dayOfMonth","current_city","hourOfDay","current_district","model","miui_release","screenResolution","age","miui_version"]
    duplicatefeature = ["deviceId","age","gender","city","province","country"]

    newcommonStrMap = list(set(commonStrMap) ^ set(duplicatefeature))
    for feature in newcommonStrMap:
        result = pd.DataFrame(df[feature].value_counts(normalize=True, dropna=False))
        result.to_csv(output, mode="a", sep="\t")

    newdf = pd.DataFrame(df.drop_duplicates(['deviceId']).reset_index(drop=True))
    for feature in duplicatefeature[1:]:
        result = pd.DataFrame(newdf[feature].value_counts(normalize=True, dropna=False))
        result.to_csv(output, mode="a", sep="\t")

    featureStrMap = ["videoWidth", "videoCategory", "sourceTags","videoSize", "nlpCategory", "videoHeight", "opRecomType", "videoSubCategory"]
    #featureStrMap = ["videoWidth", "itemId", "videoCategory", "sourceTags", "boardId", "authorId", "videoSize", "nlpCategory", "videoHeight", "opRecomType", "videoSubCategory"]
    for feature in featureStrMap:
        result = pd.DataFrame(df[feature].value_counts(normalize=True, dropna=False))
        result.to_csv(output, mode="a", sep="\t")



if __name__ == '__main__':
    #read_multicsv()
    inputpath = '../data/rawdata'
    output = "../result/rawdata.csv"
    result = "../result/result.csv"
    rawtocsv(inputpath,output)
    read_multicsv(result)

    # transcsv(inputpath)
    # readcsv()

    # transcsv_D(inputpath)
    # readcsv2()

    #{"userId":"74074526fa8e9e8a1b1d5be9c294bce3","itemId":"63:38c5300cff37ee74c5891ed125dfb70d:9223370450222207331:6385","commonStrMap":{"country":"中国","gender":"男","phone_channel_code":"46000","city":"湘潭市","phone_is_international":"是","phone_brand":"红米手机6","deviceId":"74074526fa8e9e8a1b1d5be9c294bce3","current_province":"湖南省","current_country":"中国","minute":"58","miui_big_version":"v11","dayOfWeek":"1","province":"湖南省","phone_telecom":"中国移动","dayOfMonth":"24","current_city":"湘潭市","hourOfDay":"12","current_district":"岳塘区","model":"Redmi 6","miui_release":"稳定版","screenResolution":"720x1344","age":"30-34","miui_version":"v11"},"featureIntMap":{"item12Expose":8512,"item24Skip":15,"item24Duration":43886,"item24View":120,"item12Click":664,"item72Skip":25,"item12Duration":14178,"item72View":312,"item72Expose":28321,"item72Duration":55353,"videoDuration":10800,"commentInterval":3663832,"titleLen":29,"item12Skip":3,"item24Click":1941,"item72Click":2372,"item24Expose":25267,"item12View":39},"featureDoubleMap":{"item72VideoCTR":0.08375114751783067,"item24VideoCTR":0.07681652683235714,"item12VideoCTR":0.07799835545636086},"featureStrMap":{"videoWidth":"720","itemId":"63:38c5300cff37ee74c5891ed125dfb70d:9223370450222207331:6385","videoCategory":"搞笑","sourceTags":"weishi","boardId":"38c5300cff37ee74c5891ed125dfb70d","authorId":"12489157","videoSize":"1280×720","nlpCategory":"搞笑","videoHeight":"1280","opRecomType":"0","videoSubCategory":"剧情段子"},"path":"12","duration":0,"action":"EXPOSE"}