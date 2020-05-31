import pandas as pd
import json
import matplotlib.pyplot as plt

#离散
def readrawdata(inputpath):
    featurelist = ["deviceId","age","gender","country","province","city"]
    # miss_count = dict.fromkeys(featurelist, 0)
    count = 0
    df = pd.DataFrame()
    with open(inputpath, 'r', encoding='utf-8') as file:
        for line in file.readlines():
            count = count + 1
            if count % 5000 == 0:
                print(str(count) + " lines")

            datalist = json.loads(line)
            dic = dict()
            for feature in featurelist:
                try:
                    dic[feature] = datalist['commonStrMap'][feature]
                except:
                    dic[feature] = None
            data_df = pd.DataFrame(dic, index=["0"])
            df = df.append(data_df, ignore_index = True)

    total = df.shape[0]
    df = df.drop_duplicates(['deviceId']).reset_index(drop=True)
    number = df.shape[0]

    out = open("./result/DiscreteStatistics.txt", "w+", encoding="utf-8")
    out.write("数据总数：" + str(total) + "\n")
    out.write("去重后数量：" + str(number) + "\n")
    for feature in featurelist[1:]:
        num_feature = df[feature].count()
        miss = (float(number) - float(num_feature)) * 100 / float(number)
        out.write("\n特征：" + feature + "\n")
        out.write("包含该特征数量：" + str(num_feature) + "\n")
        out.write("缺失比例：" + str('%.2f' % miss) + "%\n")
        df[feature].value_counts().to_csv("./result/"+feature+".csv", sep="\t", encoding="utf8")

    #df["age"].value_counts(normalize=True) 显示百分比
    #df["age"].value_counts().to_csv("age.csv", sep="\t",encoding="utf8")
    print("统计完成！请查看./result/DiscreteStatistics.txt")

    file.close()

def readcsv():
    featurelist = ["deviceId", "age", "gender", "country", "province", "city"]
    for key in featurelist[1:]:
        print(1)
        #draw_from_dict(feature_valuemap[key], range, 1, str(key))


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

if __name__ == '__main__':
    inputpath = './rawdata'
    readrawdata(inputpath)