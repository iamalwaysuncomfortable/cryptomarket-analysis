import matplotlib.pyplot as plt

def plot_data(inputdata, feature1, feature2):
    fig, ax = plt.subplots()
    ax.scatter(inputdata[feature1], inputdata[feature2])
    for index, entry in inputdata.iterrows():
        ax.annotate(entry["name"], (entry[feature1], entry[feature2]))
    plt.show()