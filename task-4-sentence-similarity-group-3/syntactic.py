import csv
import numpy as np
import matplotlib.pyplot as plt
from scipy.stats import pearsonr
from sklearn.linear_model import LinearRegression
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

#############################################################################
# 1) Load Data
#############################################################################
def load_sts_data_tsv(file_path):
    """
    Loads an STS-like tab-separated file where columns might be:
      0: main-captions
      1: MSRvid
      2: 2012test
      3: 0001
      4: 5.0 (similarity score)
      5: A plane is taking off.
      6: An air plane is taking off.
      (possibly more columns afterward)

    Returns three lists: sents1, sents2, scores
    """
    sents1 = []
    sents2 = []
    scores = []
    with open(file_path, "r", encoding="utf-8") as f:
        reader = csv.reader(f, delimiter="\t")
        for row_idx, row in enumerate(reader):
            # We expect at least 7 columns: [0..6]
            if len(row) < 7:
                continue
            try:
                score = float(row[4])
                sent1 = row[5]
                sent2 = row[6]
                scores.append(score)
                sents1.append(sent1)
                sents2.append(sent2)
            except ValueError:
                # If there's a header or parse error, skip it
                print(f"Skipping line {row_idx} due to parse error: {row}")
                continue
    return sents1, sents2, scores

#############################################################################
# 2) Syntactic Similarity (TF–IDF + Cosine)
#############################################################################
def compute_tfidf_similarities(sents1, sents2, vectorizer):
    """
    Given two lists of sentences (sents1[i], sents2[i]),
    and a fitted TF–IDF vectorizer, compute the cosine similarity
    for each pair. Return a list of similarity scores in [0..1].
    """
    sims = []
    for s1, s2 in zip(sents1, sents2):
        # Transform each sentence into a TF–IDF vector
        v1 = vectorizer.transform([s1])  # shape (1, vocab_size)
        v2 = vectorizer.transform([s2])  # shape (1, vocab_size)

        # Compute cosine similarity
        # cosine_similarity returns [[sim]], so take [0][0]
        sim = cosine_similarity(v1, v2)[0][0]
        sims.append(sim)
    return sims

#############################################################################
# 3) Save Predicted Scores to File
#############################################################################
def save_scores_to_file(scores, file_path):
    """
    Saves one score per line in a text file (raw predictions).
    """
    with open(file_path, "w", encoding="utf-8") as f:
        for s in scores:
            f.write(f"{s}\n")

#############################################################################
# 4) Main Pipeline for Task 2 (Purely Syntactic)
#############################################################################
def main():
    # 4.1: Load Data
    train_file = "./data/sts-train.csv"
    test_file = "./data/sts-test.csv"

    train_sents1, train_sents2, train_gt = load_sts_data_tsv(train_file)
    test_sents1, test_sents2, test_gt = load_sts_data_tsv(test_file)

    train_gt = np.array(train_gt)
    test_gt = np.array(test_gt)

    # 4.2: Fit a TF–IDF Vectorizer on the entire train set (sentences only)
    #      We combine sentence1 and sentence2 into one list for fitting the vocabulary.
    all_train_sentences = train_sents1 + train_sents2
    vectorizer = TfidfVectorizer(
        lowercase=True,
        stop_words=None, 
    )
    vectorizer.fit(all_train_sentences)

    # 4.3: Compute Syntactic Similarities on the train set
    train_pred_raw = compute_tfidf_similarities(train_sents1, train_sents2, vectorizer)

    #   4.3.1: Save raw predictions (before regression)
    save_scores_to_file(train_pred_raw, "./results/train_scores_syntactic.txt")
    #save the sentences and scores to a file
    with open("./results/train_sentences_syntactic.txt", "w") as f:
        for s1, s2, score in zip(train_sents1, train_sents2, train_pred_raw):
            f.write(f"{s1}\t{s2}\t{score}\n")

    # 4.4: Train a linear regression model to map raw similarity -> ground truth
    X_train = np.array(train_pred_raw).reshape(-1, 1)  # shape (N,1)
    y_train = train_gt
    lr_model = LinearRegression()
    lr_model.fit(X_train, y_train)

    # 4.5: Compute Syntactic Similarities on the test set
    test_pred_raw = compute_tfidf_similarities(test_sents1, test_sents2, vectorizer)

    #   4.5.1: Save raw predictions (before regression)
    save_scores_to_file(test_pred_raw, "./results/test_scores_syntactic.txt")
    #save the sentences and scores to a file
    with open("./results/test_sentences_syntactic.txt", "w") as f:
        for s1, s2, score in zip(test_sents1, test_sents2, test_pred_raw):
            f.write(f"{s1}\t{s2}\t{score}\n")

    # 4.6: Apply the regression model to get final predictions on test
    X_test = np.array(test_pred_raw).reshape(-1, 1)
    test_pred_final = lr_model.predict(X_test)

    # 4.7: Evaluate with Pearson correlation
    pearson_corr, _ = pearsonr(test_pred_final, test_gt)
    print(f"Pearson Correlation on Test (TF–IDF Syntactic) = {pearson_corr:.4f}")

    # 4.8: Plot predicted vs. ground truth
    data = sorted(zip(test_pred_final, test_gt), key=lambda p: (p[0], p[1]))
    x_sorted = [d[0] for d in data]
    y_sorted = [d[1] for d in data]

    plt.figure(figsize=(8, 5))
    plt.plot(x_sorted, y_sorted, ".")
    plt.xlabel("Predicted Score (after regression)")
    plt.ylabel("Ground Truth Score")
    plt.text(0.05, 0.95, f"Pearson Correlation: {pearson_corr:.4f}", transform=plt.gca().transAxes, fontsize=12, verticalalignment='top')
    plt.title("Purely Syntactic Predictor (Test Set)")
    plt.grid(True)
    plt.savefig("./results/syntactic_predictor.png")
    plt.show()



if __name__ == "__main__":
    main()
