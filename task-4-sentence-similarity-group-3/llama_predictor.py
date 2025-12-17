import csv
import numpy as np
import matplotlib.pyplot as plt
from scipy.stats import pearsonr
from sklearn.linear_model import LinearRegression
from ollama import chat
from ollama import ChatResponse


#############################################################################
# 1) Load Data (CSV or TSV)
#############################################################################
def load_sts_data_csv(file_path, delimiter="\t"):
    """
    Example CSV loader. Adjust if your file is TSV (use delimiter="\t").
    For a row like:
        main-captions,MSRvid,2012test,0001,5.0,A plane is taking off.,An air plane is taking off.
    we assume:
      - row[4] = similarity score
      - row[5] = sentence1
      - row[6] = sentence2
    """
    sents1 = []
    sents2 = []
    scores = []
    with open(file_path, "r", encoding="utf-8") as f:
        reader = csv.reader(f, delimiter=delimiter)
        for row_idx, row in enumerate(reader):
            if len(row) < 7:
                continue
            try:
                score = float(row[4])
                sent1 = row[5]
                sent2 = row[6]
                sents1.append(sent1)
                sents2.append(sent2)
                scores.append(score)
            except ValueError:
                # possibly a header or parse issue
                print(f"Skipping line {row_idx} due to parse error: {row}")
                continue
    return sents1, sents2, scores


#############################################################################
# 2) Ollama-based LLM similarity predictor
#############################################################################
def get_llm_similarity_ollama(sent1, sent2, model="llama2"):
    """
    Calls a local LLM via ollama CLI. Expects a numeric answer in [0,5].
    Returns a float. If parsing fails, returns None or a default value.

    Adjust the prompt to your preference. We strongly suggest
    instructing the LLM to *only output a number* for easier parsing.

    Example CLI usage:
        ollama run -m /path/to/your/model 'Your prompt here'
    """
    # Construct a robust prompt to get a single numeric answer.
    # We can give instructions like "only return the number (0..5)" etc.
    prompt_text = f"""
    You are a helpful assistant that rates semantic similarity between sentences.
    Return *only* a single decimal number between 0.0 and 1.0 representing similarity.
    0.0 means completely different, 1.0 means identical. No extra text or explanation.

    Sentence 1: {sent1}
    Sentence 2: {sent2}
    Please output the numerical similarity score between 0.0 and 1.0, and *nothing else*.
    """


    try:
        response: ChatResponse = chat(model='llama3.2', options={"temperature": 0.4}, messages=[
            {
                'role': 'user',
                'content': f'{prompt_text}',
            },
        ])
        result = response.message.content
        print(result)
        output = result

        # Attempt to parse a float from the output
        float_val = None
        for chunk in output.split():
            try:
                float_val = float(chunk)
                break
            except ValueError:
                pass

        # If we never got a float, default to None or random
        if float_val is None:
            print(f"WARNING: Could not parse float from LLM output: {output}")
            return None

        # Scale the value from [0,1] to [0,5]
        if float_val is not None:
            float_val = float_val * 5.0
        print(float_val)
        # Clip value to [0,5] for safety
        float_val = max(0.0, min(5.0, float_val))
        return float_val

    except Exception as e:
        print(f"LLM call failed: {e}")
        return None


#############################################################################
# 3) Save Scores to File
#############################################################################
def save_scores_to_file(scores,file_path):
    with open(file_path, "w", encoding="utf-8") as f:
        for s in scores:
            f.write(f"{s}")


#############################################################################
# 4) Main Pipeline
#############################################################################
def main():
    # (A) Files
    train_file = "./data/sts-train.csv"  # or .tsv
    test_file = "./data/sts-test.csv"

    # (B) Load Data
    train_sents1, train_sents2, train_gt = load_sts_data_csv(train_file)
    test_sents1, test_sents2, test_gt = load_sts_data_csv(test_file)
    train_gt = np.array(train_gt)
    test_gt = np.array(test_gt)

    # (C) LLM-based predictions for TRAIN
    train_pred_raw = []
    for s1, s2 in zip(train_sents1, train_sents2):
        val = get_llm_similarity_ollama(s1, s2, model="llama2")
        if val is None:
            val = 2.5  # fallback default if LLM fails
        train_pred_raw.append(val)

    # (C.1) Save raw LLM predictions for train
    save_scores_to_file(train_pred_raw, "./results/train_scores_llm.txt")
    #save the sentences and scores to a file
    with open("./results/train_sentences_llm.txt", "w") as f:
        for s1, s2, score in zip(train_sents1, train_sents2, train_pred_raw):
            f.write(f"{s1}\t{s2}\t{score}\n")

    # (D) Train linear regression on (train_pred_raw -> train_gt)
    X_train = np.array(train_pred_raw).reshape(-1, 1)
    y_train = train_gt
    lr_model = LinearRegression()
    lr_model.fit(X_train, y_train)

    # (E) LLM-based predictions for TEST
    test_pred_raw = []
    for s1, s2 in zip(test_sents1, test_sents2):
        val = get_llm_similarity_ollama(s1, s2, model="llama2")
        if val is None:
            val = 2.5
        test_pred_raw.append(val)

    # (E.1) Save raw LLM predictions for test
    save_scores_to_file(test_pred_raw, "./results/test_scores_llm.txt")
    #save the sentences and scores to a file
    with open("./results/test_sentences_llm.txt", "w") as f:
        for s1, s2, score in zip(test_sents1, test_sents2, test_pred_raw):
            f.write(f"{s1}\t{s2}\t{score}\n")

    # (F) Apply regression model to get final test predictions
    X_test = np.array(test_pred_raw).reshape(-1, 1)
    test_pred_final = lr_model.predict(X_test)

    # (G) Evaluate
    pearson_corr, _ = pearsonr(test_pred_final, test_gt)
    print(f"Pearson Correlation (Test) = {pearson_corr:.4f}")

    # (H) Plot
    data = sorted(zip(test_pred_final, test_gt), key=lambda x: (x[0], x[1]))
    x_sorted = [d[0] for d in data]
    y_sorted = [d[1] for d in data]

    import matplotlib.pyplot as plt
    #add pearson correlation to the plot
    plt.figure(figsize=(8, 5))
    plt.plot(x_sorted, y_sorted, ".")
    plt.text(0.05, 0.95, f"Pearson Correlation: {pearson_corr:.4f}", transform=plt.gca().transAxes, fontsize=12, verticalalignment='top')
    plt.xlabel("Predicted Score (after regression)")
    plt.ylabel("Ground Truth Score")
    plt.title("LLM-based Predictor (ollama)")
    plt.grid(True)
    plt.savefig("./results/llm_predictor.png")
    plt.show()



if __name__ == "__main__":
    main()
