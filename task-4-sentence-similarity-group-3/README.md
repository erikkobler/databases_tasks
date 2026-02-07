## Approaching STS benchmark in 4 steps:
1. predictor.py: Setting up the pipeline for STS benchmark:
   - Parser/Tokenizer
   - Score predictor: In this script, randomly guessing similarity, thus pearson corr. ~0.0
   - linear regression model: train predictions vs. train ground truth -> mapping scores fomr [0,1] to [0,5]
   - evaluation module: evaluating final model on test data
  
2. snytactic.py: Updating the score predictor to include syntactic information
3. semantic.py: Updating score predictor to include semantic information using sentence embedding
4. llama_predictor.py: Updating score predictor to use local Ollama model for prediction.
