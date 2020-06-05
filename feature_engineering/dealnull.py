from sklearn.ensemble import RandomForestClassifier
import pandas as pd
from sklearn.linear_model import SGDClassifier
from sklearn.metrics import classification_report
from sklearn.model_selection import train_test_split
from sklearn import preprocessing
import numpy as np
import time

itersize = 100000
#读取文件返回dataframe类型的数据
def readdata(path):
    # 分块读取文件
    reader = pd.read_csv(path, iterator=True)
    loop = True
    chunksize = itersize
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
    rawdata = pd.concat(chunks, ignore_index=True)
    return rawdata

# 缺失值处理
def handelnull(rawdata:pd.DataFrame):
    #age的缺失值处理:

    # 删除age为空的行
    rawdata.dropna(subset=['age'],inplace=True)
    # print(mydata['age'].isnull().sum(axis=0))

    # 众数填充空值
    # print(rawdata['age'].isnull().sum(axis=0))
    # age_mode = rawdata['age'].mode()[0]
    # rawdata['age'].fillna(age_mode, inplace=True)

# 数据数值化，返回数据和标签
def Tonumeric(rawdata:pd.DataFrame):
    # 提取label, 删除label列，提取特征
    label = rawdata['label']
    data = rawdata.drop(['label'], axis=1)

    # 将类别特征编码，字符串转化为数值
    df = pd.DataFrame(pd.DataFrame(data).astype(dtype='str'))
    cols = df.select_dtypes(include=['O']).columns.tolist()
    for col in cols:
        df[col] = preprocessing.LabelEncoder().fit_transform(df[col])

    return df, label

def RFClassifer(data,label):
    # test_size测试集合所占比例
    X_train, X_test, y_train, y_test = train_test_split(data, label, test_size=0.3, random_state=0)
    clf = RandomForestClassifier(n_estimators=100)
    # 模型 训练
    clf.fit(X_train, y_train)
    # 预测值
    y_pred = clf.predict(X_test)
    # 真实值 赋值
    y_true = y_test
    #打印结果
    print(classification_report(y_true, y_pred, digits=4))

# 处理大量数据会很慢
def SGD(data,label):
    # test_size测试集合所占比例
    X_train, X_test, y_train, y_test = train_test_split(data, label, test_size=0.3, random_state=0)
    clf = SGDClassifier()
    # 模型 训练
    clf.fit(X_train, y_train)
    # 预测值
    y_pred = clf.predict(X_test)
    # 真实值 赋值
    y_true = y_test
    #打印结果
    # print(clf.score(X_test, y_test))
    print(classification_report(y_true, y_pred, digits=4))

# SGD增量学习
def IncrementalSGD(data,label):
    sgd_clf = SGDClassifier()  # SGDClassifier的参数设置可以参考sklearn官网
    X_train, X_test, y_train, y_test = train_test_split(data, label, test_size=0.3, random_state=0)
    interval = itersize
    start = 0
    y_pred = []
    y_true = []
    for i in np.arange(1, (X_train.shape[0] // interval + 1), 1):
        end = min([i * interval, X_train.shape[0]])
        X = X_train[start:end]
        Y = y_train[start:end]
        sgd_clf.partial_fit(X, Y, classes=[0,1])  #
        start = end

        # 预测值
        y_pred = sgd_clf.predict(X_test)
        # 真实值 赋值
        y_true = y_test
    # 打印结果
    print(classification_report(y_true, y_pred, digits=4))

if __name__ == '__main__':
    inputpath="../data/out20200601.csv"
    rawdata = pd.DataFrame(readdata(inputpath))
    # handelnull(rawdata)
    handelnull(rawdata)
    data, label = Tonumeric(rawdata)

    time_start = time.time()
    #RFClassifer(data, label)
    IncrementalSGD(data, label)
    time_end = time.time()
    print('totally cost', time_end - time_start)