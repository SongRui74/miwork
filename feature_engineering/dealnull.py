import json
from sklearn.feature_selection import VarianceThreshold
import pandas as pd

def readdata(path):
    # file = open(path, 'r', encoding='utf-8')
    # print(file.readline())

    rawdata = pd.read_table(path, iterator=True, header=None)
    data = pd.DataFrame(rawdata)

    print(type(data))

    # while True:
    #     try:
    #         chunk = train_data.get_chunk(5600000)
    #         chunk.columns = ['user_id', 'spu_id', 'buy_or_not', 'date']
    #         chunk.to_csv('big_data.csv', mode='a', header=False, index=None)
    #     except Exception as e:
    #         break
    #
# X = [[0, 0, 1], [0, 1, 0], [1, 0, 0], [0, 1, 1], [0, 1, 0], [0, 1, 1]]
# sel = VarianceThreshold(threshold=(.8 * (1 - .8)))
# sel.fit_transform(X)

if __name__ == '__main__':
    readdata("../rawdata")