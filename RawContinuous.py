import pandas as pd
import json

#连续
def readrawdata(inputpath):
    feature = "videoDuration"
    count = 0
    df = pd.DataFrame(dtype='float')
    with open(inputpath, 'r', encoding='utf-8') as file:
        for line in file.readlines():
            count = count + 1
            if count % 5000 == 0:
                print(str(count) + " lines")

            datalist = json.loads(line)
            dic = dict()
            try:
                dic[feature] = datalist['featureIntMap'][feature]
            except:
                dic[feature] = None

            data_df = pd.DataFrame(dic, index=["0"], dtype='float')
            df = df.append(data_df, ignore_index = True)

    num_feature = df["videoDuration"].count()
    number = df.shape[0]
    miss = (float(number) - float(num_feature)) *100 / float(number)
    out = open("./result/ContinuousStatistics.txt","w+",encoding="utf-8")
    out.write("特征：" + str(feature) + "\n")
    out.write("数据总数：" + str(number)+"\n")
    out.write("包含该特征数量：" + str(num_feature) + "\n")
    out.write("缺失比例：" + str('%.2f' % miss) + "%\n")
    out.write(str(df.describe(percentiles=[.1,.2,.3,.4,.5,.6,.7,.8,.9])))

    print("统计完成！请查看./result/ContinuousStatistics.txt")
    # print(df.describe(percentiles=[.1,.2,.3,.4,.5,.6,.7,.8,.9]))
    file.close()

if __name__ == '__main__':
    inputpath = './rawdata'
    readrawdata(inputpath)