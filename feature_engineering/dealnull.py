import json
from sklearn.feature_selection import VarianceThreshold

def readdata(path):
    file = open(path, 'r', encoding='utf-8')
    print(file.readline())
    print(file.readline())

# X = [[0, 0, 1], [0, 1, 0], [1, 0, 0], [0, 1, 1], [0, 1, 0], [0, 1, 1]]
# sel = VarianceThreshold(threshold=(.8 * (1 - .8)))
# sel.fit_transform(X)

if __name__ == '__main__':
    readdata("../task-32323461-stdout")