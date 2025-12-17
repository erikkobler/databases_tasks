import csv
import random
import numpy as np
import matplotlib.pyplot as plt
from scipy.stats import pearsonr
from sklearn.linear_model import LinearRegression


#############################################################################
# 1) Load Data (CSV)
#############################################################################
def load_sts_data_csv(file_path):
    """
    Loads an STS-like CSV file with columns, for example:
      0: main-captions
      1: MSRvid
      2: 2012test
      3: 0001
      4: 5.000 (similarity score)
      5: A plane is taking off.
      6: An air plane is taking off.
      7: (possibly empty)
      ...
    Returns three lists: sents1, sents2, scores
    """
    sents1 = []
    sents2 = []
    scores = []

    with open(file_path, "r", encoding="utf-8") as f:
        reader = csv.reader(f,delimiter="\t")
        for row_idx, row in enumerate(reader):
            # Skip empty lines if any
            if not row or len(row) < 7:
                continue

            try:
                # Score is in column 4
                score_str = row[4]
                score = float(score_str)

                # Sentence1 is in column 5
                sent1 = row[5]

                # Sentence2 is in column 6
                sent2 = row[6]

                sents1.append(sent1)
                sents2.append(sent2)
                scores.append(score)
            except ValueError:
                # If there's a header or invalid row,
                # you can skip it or handle it in some special way
                print(f"Skipping line {row_idx} due to parse error: {row}")
                continue

    return sents1, sents2, scores


#############################################################################
# 2) Random Uniform Predictor
#############################################################################
def random_uniform_predictor(num_pairs, low=0.0, high=5.0):
    """
    Produces random scores uniformly in [low, high].
    """
    random_scores = []
    for _ in range(num_pairs):
        random_scores.append(random.uniform(low, high))
    return random_scores


#############################################################################
# 3) Save Predicted Scores to File
#############################################################################
def save_scores_to_file(scores, file_path):
    """
    Saves one score per line in a text file.
    """
    with open(file_path, "w", encoding="utf-8") as f:
        for s in scores:
            f.write(f"{s}\n")


#############################################################################
# 4) Main Pipeline: Train -> Predict -> Evaluate
#############################################################################
def main():
    # (A) Paths
    train_file = "./data/sts-train.csv"
    test_file = "./data/sts-test.csv"

    # (B) Load data
    train_sents1, train_sents2, train_gt = load_sts_data_csv(train_file)
    test_sents1, test_sents2, test_gt = load_sts_data_csv(test_file)

    train_gt = np.array(train_gt)
    test_gt = np.array(test_gt)

    # (C) Predict train scores (random)
    train_pred_raw = random_uniform_predictor(len(train_sents1), low=0, high=5)

    # (C.1) Save raw predictions for the train set (BEFORE regression)
    save_scores_to_file(train_pred_raw, "./results/train_scores_guess.txt")
    #save the sentences and scores to a file
    with open("./results/train_sentences_guess.txt", "w") as f:
        for s1, s2, score in zip(train_sents1, train_sents2, train_pred_raw):
            f.write(f"{s1}\t{s2}\t{score}\n")

    # (D) Train a linear regression model
    # We want to fit train_pred_raw -> train_gt
    X_train = np.array(train_pred_raw).reshape(-1, 1)  # shape (N,1)
    y_train = train_gt  # shape (N,)

    lr_model = LinearRegression()
    lr_model.fit(X_train, y_train)

    # (D.1) Check learned parameters
    print("Coefficients:", lr_model.coef_, "Intercept:", lr_model.intercept_)

    # (E) Predict test scores (random)
    test_pred_raw = random_uniform_predictor(len(test_sents1), low=0, high=5)

    # (E.1) Save raw predictions for the test set (BEFORE regression)
    save_scores_to_file(test_pred_raw, "./results/test_scores_guess.txt")
    #save the sentences and scores to a file
    with open("./results/test_sentences_guess.txt", "w") as f:
        for s1, s2, score in zip(test_sents1, test_sents2, test_pred_raw):
            f.write(f"{s1}\t{s2}\t{score}\n")
    
    # (F) Apply the regression model to test scores
    X_test = np.array(test_pred_raw).reshape(-1, 1)
    test_pred_final = lr_model.predict(X_test)  # shape (N,)

    # (G) Evaluate test predictions
    # 1) Pearson correlation
    pearson_corr, _ = pearsonr(test_pred_final, test_gt)
    print(f"Pearson Correlation (Test) = {pearson_corr:.4f}")

    # 2) Plot predicted vs. ground truth
    # Sort by predicted (x-axis), then by ground truth
    data = sorted(zip(test_pred_final, test_gt), key=lambda p: (p[0], p[1]))
    x_sorted = [d[0] for d in data]
    y_sorted = [d[1] for d in data]

    plt.figure(figsize=(8, 5))
    plt.plot(x_sorted, y_sorted, ".")
    plt.xlabel("Predicted Score (final, after regression)")
    plt.ylabel("Ground Truth Score")
    plt.title("Random Uniform Predictor (Test Set)")
    plt.text(0.05, 0.95, f"Pearson Correlation: {pearson_corr:.4f}", transform=plt.gca().transAxes, fontsize=12, verticalalignment='top')
    plt.grid(True)
    plt.savefig("./results/random_uniform_predictor.png")
    plt.show()



if __name__ == "__main__":
    main()
