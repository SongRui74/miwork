import csv

def transcsv(inputpath,output):
    #增加了label列！
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

    #分桶后特征列表：
    newbucketFeatures = list()
    for item in bucketFeatures:
        for i in range(0,5):
            newfeature = str(item)+'_bucket_'+str(i)
            newbucketFeatures.append(newfeature)

    # 除交叉特征之外的所有特征列表：
    oringinFeatures = list(set(features) ^ set(bucketFeatures))
    featurelist = oringinFeatures + newbucketFeatures
   # featurelist = oringinFeatures
    csv_file = open(output, 'w', encoding='utf-8', newline="")  # 默认newline='\n'
    writer = csv.writer(csv_file)
    writer.writerow(featurelist)
    number = 0
    with open(inputpath, 'r', encoding='utf-8') as file:
        for line in file.readlines():
            number = number + 1
            if number % 20000 == 0:
                print(number)
               # break
            data = linetodic(line)
            value = list()
            for feature in featurelist:
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
        else:
            feature = item[0:item.index(":")]
            value = item[item.index(":") + 1:]
            dic[feature] = value
    return dic

if __name__ == '__main__':
    inputpath = "../data/feature20200601"
    datapath = "../data/out20200601.csv"
    transcsv(inputpath,datapath)