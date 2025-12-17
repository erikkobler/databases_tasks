import csv
import numpy as np
import matplotlib.pyplot as plt
from scipy.stats import pearsonr
from sklearn.linear_model import LinearRegression
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity


#############################################################################
# 1) Load Data
#############################################################################
def load_sts_data_csv(file_path):
    """
    Example CSV loader. Adjust if your file is TSV.
    For a CSV row like:
        main-captions,MSRvid,2012test,0001,5.0,A plane is taking off.,An air plane is taking off.
    we assume:
      - row[4] = similarity score
      - row[5] = sentence1
      - row[6] = sentence2
    Returns (sents1, sents2, scores).
    """
    sents1 = []
    sents2 = []
    scores = []

    with open(file_path, "r", encoding="utf-8") as f:
        reader = csv.reader(f, delimiter="\t")  # use delimiter="\t" if TSV
        for row_idx, row in enumerate(reader):
            # Skip lines that do not have at least 7 columns
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
                # Possibly a header row or parse error
                print(f"Skipping line {row_idx} due to parse error: {row}")
                continue

    return sents1, sents2, scores


#############################################################################
# 2) Sentence Embedding + Cosine Similarity
#############################################################################
def compute_semantic_similarities(sents1, sents2, model):
    """
    Given two lists of sentences (sents1[i], sents2[i]) and a SentenceTransformer model,
    compute the sentence embeddings, then the cosine similarity for each pair.
    Return a list of similarity scores (usually in [0..1] if sentences are somewhat similar).
    """
    # Step 1: Encode all sentences in a batch for efficiency
    # We'll encode sents1 + sents2 in one go, then separate
    all_sentences = sents1 + sents2
    all_embeddings = model.encode(all_sentences, batch_size=32, show_progress_bar=True)
    # The first len(sents1) embeddings correspond to sents1
    # The next len(sents2) embeddings correspond to sents2
    half = len(sents1)
    emb_sents1 = all_embeddings[:half]
    emb_sents2 = all_embeddings[half:]

    # Step 2: Compute similarity for each pair
    sims = []
    for i in range(half):
        # Compute the dot-product-based cos similarity
        # we can just reshape them to (1, -1) and use sklearn's cosine_similarity
        v1 = emb_sents1[i].reshape(1, -1)
        v2 = emb_sents2[i].reshape(1, -1)
        sim = cosine_similarity(v1, v2)[0][0]  # single number
        sims.append(sim)
    return sims


#############################################################################
# 3) Save Scores
#############################################################################
def save_scores_to_file(scores, file_path):
    with open(file_path, "w", encoding="utf-8") as f:
        for s in scores:
            f.write(f"{s}\n")


#############################################################################
# 4) Main: Using Sentence Embeddings for STS
#############################################################################
def main():
    # (A) Paths
    train_file = "./data/sts-train.csv"
    test_file = "./data/sts-test.csv"

    # (B) Load Data
    train_sents1, train_sents2, train_gt = load_sts_data_csv(train_file)
    test_sents1, test_sents2, test_gt = load_sts_data_csv(test_file)
    train_gt = np.array(train_gt)
    test_gt = np.array(test_gt)

    # (C) Load or Initialize a Sentence Embedding Model
    #model_name = "sentence-transformers/all-MiniLM-L6-v2"
    #model_name = "sentence-transformers/distiluse-base-multilingual-cased-v2"
    model_name = "BAAI/bge-m3"
    model = SentenceTransformer(model_name)

    # (D) Compute semantic similarities on the TRAIN set
    train_pred_raw = compute_semantic_similarities(train_sents1, train_sents2, model)

    # (D.1) Save raw predictions (before regression)
    save_scores_to_file(train_pred_raw, "./results/train_scores_semantic.txt")
    #save the sentences and scores to a file
    with open("./results/train_sentences_semantic.txt", "w") as f:
        for s1, s2, score in zip(train_sents1, train_sents2, train_pred_raw):
            f.write(f"{s1}\t{s2}\t{score}\n")

    # (E) Train a linear regression model (mapping raw similarity -> ground truth)
    X_train = np.array(train_pred_raw).reshape(-1, 1)
    y_train = train_gt
    lr_model = LinearRegression()
    lr_model.fit(X_train, y_train)

    # (F) Compute semantic similarities on the TEST set
    test_pred_raw = compute_semantic_similarities(test_sents1, test_sents2, model)

    # (F.1) Save raw predictions (before regression)
    save_scores_to_file(test_pred_raw, "./results/test_scores_semantic.txt")
    #save the sentences and scores to a file
    with open("./results/test_sentences_semantic.txt", "w") as f:
        for s1, s2, score in zip(test_sents1, test_sents2, test_pred_raw):
            f.write(f"{s1}\t{s2}\t{score}\n")

    # (G) Apply the regression model to get final scores
    X_test = np.array(test_pred_raw).reshape(-1, 1)
    test_pred_final = lr_model.predict(X_test)

    # (H) Evaluate (Pearson correlation)
    pearson_corr, _ = pearsonr(test_pred_final, test_gt)
    print(f"Pearson Correlation (Test) = {pearson_corr:.4f}")

    # (I) Plot predicted vs. ground truth
    data = sorted(zip(test_pred_final, test_gt), key=lambda p: (p[0], p[1]))
    x_sorted = [d[0] for d in data]
    y_sorted = [d[1] for d in data]

    plt.figure(figsize=(8, 5))
    plt.plot(x_sorted, y_sorted, ".")
    plt.xlabel("Predicted Score (after regression)")
    plt.ylabel("Ground Truth Score")
    plt.title("Semantic Predictor (Sentence Embeddings)")
    plt.text(0.05, 0.95, f"Pearson Correlation: {pearson_corr:.4f}", transform=plt.gca().transAxes, fontsize=12, verticalalignment='top')
    plt.grid(True)
    plt.savefig("./results/semantic_predictor.png")
    plt.show()
    plt.close()


if __name__ == "__main__":
    main()
