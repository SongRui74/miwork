import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn import preprocessing

def readdata(path)->pd.DataFrame:
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

    return pd.DataFrame(data)

def featureimportance(data:pd.DataFrame,output):
    # 提取label, 删除label列，提取特征
    target = data['label']
    data = data.drop(['label'], axis=1)
    feature = data.columns.values.tolist()

    # 将类别特征编码，字符串转化为数值
    df = pd.DataFrame(pd.DataFrame(data).astype(dtype='str'))
    cols = df.select_dtypes(include=['O']).columns.tolist()
    for col in cols:
        df[col] = preprocessing.LabelEncoder().fit_transform(df[col])

    # 分析特征重要性
    Forest_model = RandomForestRegressor(n_estimators=100)
    Forest_model.fit(df, target)
    importances = Forest_model.feature_importances_
    feature_important = pd.Series(importances, index=feature).sort_values(ascending=False)

    feature_important.to_csv(output,sep="\t")
    print(feature_important)

if __name__ == '__main__':

    datapath = "../data/out20200601.csv"
    resultpath = "../result/20200601.csv"
    data = readdata(datapath)
    featureimportance(data,resultpath)

    # str = "1 authorId@29111770:1.0 item12Expose_bucket_3:0.30663944102426166 item24Skip_bucket_1:0.7781512503836436 item24Click_bucket_2:0.9370161074648142 videoWidth@720:1.0 item72Duration_bucket_4:0.87301817642006 age_cross_gender@×:1.0 item12Duration_bucket_3:0.7688600008429569 model@Redmi5:1.0 videoSize@1280×720:1.0 item72View_bucket_2:0.8202014594856402 itemId@28-35dc3634e32620298e4759e4a9ca9cab-9223370446900895421-8002:1.0 item72Click_bucket_3:0.5420781463356255 videoDuration_bucket_3:0.9489994540269531 item12VideoCTR:0.13369511593487912 item72Expose_bucket_4:0.46466835529290407 opRecomType@0:1.0 item24Expose_bucket_3:0.8290463368531826 titleLen:7.0 item72VideoCTR:0.11950742633691215 dayOfMonth@1:1.0 videoCategory@美女:1.0 item24VideoCTR:0.1282051282051282 item72Skip_bucket_2:0.18184358794477262 screenResolution@720*1344:1.0 item12View_bucket_1:0.6989700043360187 item12Skip_bucket_1:0.04139268515822514 boardId@35dc3634e32620298e4759e4a9ca9cab:1.0 commentInterval_bucket_6:0.01314154583996352 deviceId@9184f68b34a045e0e97174a2ac70c158:1.0 item12Click_bucket_2:0.43296929087440583 hourOfDay@12:1.0 sourceTags@douyin:1.0 videoHeight@1280:1.0 dayOfWeek@2:1.0 item24View_bucket_2:0.3926969532596658 videoSubCategory@null:1.0 item24Duration_bucket_4:0.2677113267266922"
    #
    # print(linetodic(str))


