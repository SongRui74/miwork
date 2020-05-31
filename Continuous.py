import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import json

#连续
def readrawdata(inputpath):
    feature = "videoDuration"
    miss = 0
    valuelist = []
    with open(inputpath, 'r', encoding='utf-8') as file:
        data = []
        for line in file.readlines():
            dic = json.loads(line)
            data.append(dic)
            try:
                valuelist.append(dic['featureIntMap'][feature])
            except:
                miss = miss + 1

    npvalue = np.asarray(valuelist)
    print(npvalue)

    print("Feature:" + feature)
    #最大值
    max = np.amax(npvalue)
    print("最大值：" + str(max))
    #最小值
    min = np.amin(npvalue)
    print("最小值：" + str(min))
    #极差
    print("极差：" + str(np.ptp(npvalue)))

    ntile_dict = dict()
    #分位数, 10%=5388 说明 有10%的数据小于等于5388
    for i in [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]:
        ntile_dict[i] = np.percentile(npvalue, i)
        print(str(i)+"%分位数：" + str(np.percentile(npvalue, i)))

    total_number = len(data)
    miss_ratio = 100 * float(miss) / float(total_number)
    print("\n数据总数" + str(total_number))
    print("包含feature数量：" + str(len(valuelist)))
    print("缺失feature数量：" + str(miss) + "\t缺失比例：" + str('%.2f' % miss_ratio) + "%")
    file.close()

    #draw_from_dict(ntile_dict, max, heng=0, titlename = feature+"分位数")


if __name__ == '__main__':
    inputpath = './rawdata'
    readrawdata(inputpath)