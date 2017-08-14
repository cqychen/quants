import numpy as np
from sklearn.preprocessing import OneHotEncoder

enc = OneHotEncoder()
enc.fit([[0, 0, 3], [1, 1, 0], [0, 2, 1],[1, 0, 2]])
print "enc.n_values_ is:",enc.n_values_
print "enc.feature_indices_ is:",enc.feature_indices_
print enc.transform([[0, 1, 1]]).toarray()