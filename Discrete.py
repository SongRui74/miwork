import matplotlib.pyplot as plt
import re
import json

#离散
def readdata(inputpath):
    f = open(inputpath, "r", encoding="utf-8")
    number = 0  # 数据总数
    devicelist = list()
    feature_valuemap = dict()  #dict(str,dict())  {gender:{男：5,女：1}，age...}
    featurelist = ["videoSubCategory"]  #存放离散feature
    for line in f:
        toks = line.split(" ")
        deviceId = re.findall(r'itemId@(.+?):', line)
        if deviceId not in devicelist:
            devicelist.append(deviceId)
            if len(toks) > 0:
                number = number + 1
                if number % 50000 == 0:
                    print(str(number) + " lines")
                for tok in toks:
                    tmp = re.split(':|@', tok.strip())
                    if len(tmp) > 1:
                        for feature in featurelist:
                            if feature not in feature_valuemap:
                                feature_valuemap[feature] = dict()
                            if tmp[0].__eq__(feature):
                                value = tmp[1]
                                if value not in feature_valuemap[feature]:
                                    feature_valuemap[feature][value] = 1
                                else:
                                    feature_valuemap[feature][value] = feature_valuemap[feature][value] + 1
            else:
                continue

    out = open("./out.txt", 'w+', encoding="utf-8")
    miss = open("./miss.txt", 'w+', encoding="utf-8")
    out.write("去重后数据总数：" + str(number) + "\n")
    miss.write("去重后数据总数：" + str(number) + "\n")
    for key in feature_valuemap.keys():
        num_contains_feature = sum(feature_valuemap[key].values())
        num_missing_feature = number - num_contains_feature
        miss_ratio = 100 * float(num_missing_feature) / float(number)

        out.write("\n" + str(key)+"\n"+"出现次数：" + str(num_contains_feature) + "\n")
        out.write("缺失条目：" + str(num_missing_feature) + "\t" + "缺失比例："+ str('%.2f' % miss_ratio)+"%" + "\n")

        miss.write("\n" + str(key) + "\n" + "出现次数：" + str(num_contains_feature) + "\n")
        miss.write("缺失条目：" + str(num_missing_feature) + "\t" + "缺失比例：" + str('%.2f' % miss_ratio) + "%" + "\n")

        out.write("数值\t个数\t比例\n")
        for v in feature_valuemap[key]:
            ratio = 100 * float(feature_valuemap[key][v]) / float(num_contains_feature)
            out.write(str(v)+"\t" + str(feature_valuemap[key][v]) + "\t" + str('%.2f' % ratio)+"%" + "\n")

    # #绘图
    # for key in ["gender", "age", "country", "province"]:
    #     range = feature_valuemap[key][max(feature_valuemap[key].keys(), key=(lambda k: feature_valuemap[key][k]))]
    #     draw_from_dict(feature_valuemap[key], range, 1, str(key))

    print("统计完成！缺失情况查看./miss.txt,详细统计查看./out.txt\n")
    out.close()
    miss.close()
    f.close()

def draw_from_dict(dicdata, RANGE, heng=0, titlename='name'):
    #dicdata：字典的数据。
    #RANGE：截取显示的字典的长度。
    #heng=0，代表条状图的柱子是竖直向上的。heng=1，代表柱子是横向的。考虑到文字是从左到右的，让柱子横向排列更容易观察坐标轴。
    plt.figure(figsize=(10, 10))
    plt.rcParams['font.sans-serif'] = ['SimHei']  # 显示中文标签
    plt.rcParams['axes.unicode_minus'] = False
    plt.title(titlename, fontsize=14)
    by_value = sorted(dicdata.items(),key = lambda item:item[1],reverse=True)

    x = []
    y = []
    for d in by_value:
        x.append(d[0])
        y.append(d[1])
    if heng == 0:
        plt.bar(x[0:RANGE], y[0:RANGE])
        plt.show()
        return
    elif heng == 1:
        plt.barh(x[0:RANGE], y[0:RANGE])
        plt.show()
        return
    else:
        return "heng的值仅为0或1！"

def readrawdata(inputpath):
    #featureIntMap
    feature = "videoDuration"
    with open(inputpath, 'r', encoding='utf-8') as file:
        data = []
        for line in file.readlines():
            dic = json.loads(line)
            #json有空值？
            print(dic['featureIntMap'][feature])
            data.append(dic)
    file.close()

if __name__ == '__main__':
    inputpath = './data'
    readdata(inputpath)