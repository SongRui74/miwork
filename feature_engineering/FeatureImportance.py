import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn import datasets, preprocessing
import numpy as np
import csv

#分桶后的特征列表
newbucketFeatures = list()

def test():
    Forest_model = RandomForestRegressor(n_estimators=100)
    iris = datasets.load_iris()
    Forest_model.fit(iris.data, iris.target)
    importances = Forest_model.feature_importances_
    print(importances)
    feature_important = pd.Series(importances, index=iris.feature_names).sort_values(ascending=False)
    print(feature_important)

def transcsv(inputpath,output):
    #增加了label列！！！！！！
    features = ["label","itemId", "boardId", "authorId", "titleLen", "videoDuration", "videoWidth", "videoHeight", "videoSize",
                "opRecomType", "videoCategory", "videoSubCategory", "sourceTags", "commentProvince", "commentCity",
                "commentDistrict", "commentInterval", "item12Expose", "item12Click", "item12Like", "item12View",
                "item12Dislike", "item12Skip", "item12VideoFinish", "item12Duration", "item12VideoCTR",
                "item12VideoFTR", "item24Expose", "item24Click", "item24Like", "item24View", "item24Dislike",
                "item24Skip", "item24VideoFinish", "item24Duration", "item24VideoCTR", "item24VideoFTR", "item72Expose",
                "item72Click", "item72Like", "item72View", "item72Dislike", "item72Skip", "item72VideoFinish",
                "item72Duration", "item72VideoCTR", "item72VideoFTR", "country", "province", "gender", "age", "city",
                "current_province", "current_country", "current_city", "current_district", "deviceId", "model",
                "screenResolution", "dayOfMonth", "dayOfWeek", "hourOfDay", "clickFromItemId", "clickFromItemCategory",
                "clickFromPath"]

    bucketFeatures = ["videoDuration", "commentInterval", "item12Expose", "item12Click", "item12Like", "item12View",
                      "item12Dislike", "item12Skip", "item12VideoFinish", "item12Duration", "item24Expose",
                      "item24Click", "item24Like", "item24View", "item24Dislike", "item24Skip", "item24VideoFinish",
                      "item24Duration", "item72Expose", "item72Click", "item72Like", "item72View", "item72Dislike",
                      "item72Skip", "item72VideoFinish", "item72Duration"]

    crossFeatures = ["age_cross_gender", "age_boardId", "age_authorId", "age_videoCategory", "age_videoSubCategory",
                     "age_videoDuration", "age_titleLen", "age_opRecomType", "age_sourceTags", "age_videoSize",
                     "age_nlpCategory", "age_commentCity", "age_commentInterval", "age_commentProvince",
                     "gender_boardId", "gender_authorId", "gender_videoCategory", "gender_videoSubCategory",
                     "gender_videoDuration", "gender_titleLen", "gender_opRecomType", "gender_sourceTags",
                     "gender_videoSize", "gender_nlpCategory", "gender_commentCity", "gender_commentInterval",
                     "gender_commentProvince", "province_boardId", "province_authorId", "province_videoCategory",
                     "province_videoSubCategory", "province_videoDuration", "province_titleLen", "province_opRecomType",
                     "province_sourceTags", "province_videoSize", "province_nlpCategory", "province_commentCity",
                     "province_commentInterval", "province_commentProvince", "city_boardId", "city_authorId",
                     "city_videoCategory", "city_videoSubCategory", "city_videoDuration", "city_titleLen",
                     "city_opRecomType", "city_sourceTags", "city_videoSize", "city_nlpCategory", "city_commentCity",
                     "city_commentInterval", "city_commentProvince", "current_city_boardId", "current_city_authorId",
                     "current_city_videoCategory", "current_city_videoSubCategory", "current_city_videoDuration",
                     "current_city_titleLen", "current_city_opRecomType", "current_city_sourceTags",
                     "current_city_videoSize", "current_city_nlpCategory", "current_city_commentCity",
                     "current_city_commentInterval", "current_city_commentProvince", "current_province_boardId",
                     "current_province_authorId", "current_province_videoCategory", "current_province_videoSubCategory",
                     "current_province_videoDuration", "current_province_titleLen", "current_province_opRecomType",
                     "current_province_sourceTags", "current_province_videoSize", "current_province_nlpCategory",
                     "current_province_commentCity", "current_province_commentInterval",
                     "current_province_commentProvince"]

    oringinFeatures = list(set(features) ^ set(bucketFeatures))

    csv_file = open(output, 'w', encoding='utf-8', newline="")  # 默认newline='\n'
    writer = csv.writer(csv_file)
    writer.writerow(oringinFeatures)
    number = 0
    with open(inputpath, 'r', encoding='utf-8') as file:
        for line in file.readlines():
            number = number + 1
            if number % 20000 == 0:
                print(number)
               # break
            data = linetodic(line)
            value = list()
            for feature in oringinFeatures:
                try:
                    value.append(data[feature])
                except:
                    value.append(None)
            writer.writerow(value)
    csv_file.close()
    file.close()

def linetodic(line):
    data = line.strip().split()
    dic = dict()
    dic["label"] = data[0]
    for item in data[1:]:
        if "@" in item:
            feature = item[0:item.index("@")]
            value = item[item.index("@") + 1:item.index(":")]
            dic[feature] = value
        elif "_bucket" in item:
            feature = item[0:item.index(":")]
            value = item[item.index(":") + 1:]
            dic[feature] = value
            newbucketFeatures.append(feature)
        else:
            feature = item[0:item.index(":")]
            value = item[item.index(":") + 1:]
            dic[feature] = value
    return dic

def readdata(path):
    # 分块读取文件
    reader = pd.read_csv(path, iterator=True)
    loop = True
    chunksize = 6000
    chunks = []
    i = 0
    while loop:
        try:
            chunk = reader.get_chunk(chunksize)
            chunks.append(chunk)
            i = i + 1
            print(str(i)+"th chunk")
        except StopIteration:
            loop = False
            print("Iteration is stopped.")
    data = pd.concat(chunks, ignore_index=True)

    return data

def featureimportance(data):
    # 提取label, 删除label列，提取特征
    target = data['label']
    data = data.drop(['label'], axis=1)
    feature = data.columns.values.tolist()

    # 将类别特征编码，字符串转化为数值
    df = pd.DataFrame(data).astype(dtype='str')
    cols = df.select_dtypes(include=['O']).columns.tolist()
    for col in cols:
        df[col] = preprocessing.LabelEncoder().fit_transform(df[col])

    # 分析特征重要性
    Forest_model = RandomForestRegressor(n_estimators=100)
    Forest_model.fit(df, target)
    importances = Forest_model.feature_importances_
    feature_important = pd.Series(importances, index=feature).sort_values(ascending=False)
    print(feature_important)


if __name__ == '__main__':

    # inputpath = "./data/feature20200601"
    # output = "./data/out20200601.csv"
    # # transcsv(inputpath,output)
    # data = readdata(output)
    # featureimportance(data)

    str = "1 authorId@29111770:1.0 item12Expose_bucket_3:0.30663944102426166 item24Skip_bucket_1:0.7781512503836436 item24Click_bucket_2:0.9370161074648142 videoWidth@720:1.0 item72Duration_bucket_4:0.87301817642006 age_cross_gender@×:1.0 item12Duration_bucket_3:0.7688600008429569 model@Redmi5:1.0 videoSize@1280×720:1.0 item72View_bucket_2:0.8202014594856402 itemId@28-35dc3634e32620298e4759e4a9ca9cab-9223370446900895421-8002:1.0 item72Click_bucket_3:0.5420781463356255 videoDuration_bucket_3:0.9489994540269531 item12VideoCTR:0.13369511593487912 item72Expose_bucket_4:0.46466835529290407 opRecomType@0:1.0 item24Expose_bucket_3:0.8290463368531826 titleLen:7.0 item72VideoCTR:0.11950742633691215 dayOfMonth@1:1.0 videoCategory@美女:1.0 item24VideoCTR:0.1282051282051282 item72Skip_bucket_2:0.18184358794477262 screenResolution@720*1344:1.0 item12View_bucket_1:0.6989700043360187 item12Skip_bucket_1:0.04139268515822514 boardId@35dc3634e32620298e4759e4a9ca9cab:1.0 commentInterval_bucket_6:0.01314154583996352 deviceId@9184f68b34a045e0e97174a2ac70c158:1.0 item12Click_bucket_2:0.43296929087440583 hourOfDay@12:1.0 sourceTags@douyin:1.0 videoHeight@1280:1.0 dayOfWeek@2:1.0 item24View_bucket_2:0.3926969532596658 videoSubCategory@null:1.0 item24Duration_bucket_4:0.2677113267266922"

    print(linetodic(str))


