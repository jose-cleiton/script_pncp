#!/usr/bin/env python3
"""
train_clean.py

A clean training script (copy of the intended `train.py`) to avoid the
corrupted/duplicated `train.py` in the repository. Use this to run training
immediately.

Usage example:
    python3 train_clean.py --data data/treinamento/2026-03-14/dataset.csv \
        --model_name neuralmind/bert-base-portuguese-cased \
        --output_dir modelo_classificador --epochs 3 --batch_size 8
"""
from __future__ import annotations

import argparse
import logging
import os
import random
from typing import Dict

import numpy as np
import pandas as pd

try:
    import torch
except Exception:
    torch = None

from datasets import Dataset as HfDataset
from sklearn.metrics import accuracy_score, precision_recall_fscore_support
from transformers import (
    AutoModelForSequenceClassification,
    AutoTokenizer,
    DataCollatorWithPadding,
    Trainer,
    TrainingArguments,
    set_seed,
)

logger = logging.getLogger(__name__)


def compute_metrics(eval_pred) -> Dict[str, float]:
    logits, labels = eval_pred
    preds = np.argmax(logits, axis=1)
    precision, recall, f1, _ = precision_recall_fscore_support(
        labels, preds, average="binary", zero_division=0
    )
    acc = accuracy_score(labels, preds)
    return {"accuracy": acc, "precision": precision, "recall": recall, "f1": f1}


def set_global_seed(seed: int) -> None:
    random.seed(seed)
    np.random.seed(seed)
    if torch is not None:
        torch.manual_seed(seed)
        if torch.cuda.is_available():
            torch.cuda.manual_seed_all(seed)
    set_seed(seed)


def load_and_prepare_dataset(path: str) -> HfDataset:
    df = pd.read_csv(path)
    # support columns 'Objeto'/'Relevante' like the notebook or 'text'/'label'
    if "Objeto" in df.columns and "Relevante" in df.columns:
        df = df[["Objeto", "Relevante"]].rename(
            columns={"Objeto": "text", "Relevante": "label"}
        )
    elif "text" in df.columns and "label" in df.columns:
        df = df[["text", "label"]]
    else:
        raise ValueError(
            "Dataset must contain columns 'Objeto'/'Relevante' or 'text'/'label'"
        )

    df["label"] = (
        df["label"].astype(str).str.strip().str.replace(r"\s+", "", regex=True)
    )
    df = df[df["label"].isin(["0", "1"])].copy()
    df["label"] = df["label"].astype(int)

    df["text"] = df["text"].astype(str).str.strip()
    df = df[df["text"] != ""].copy()
    df = df.reset_index(drop=True)
    return HfDataset.from_pandas(df)


def main() -> None:
    parser = argparse.ArgumentParser(description="Train BERT classifier")
    parser.add_argument("--data", required=True)
    parser.add_argument("--model_name", default="neuralmind/bert-base-portuguese-cased")
    parser.add_argument("--output_dir", default="modelo_classificador")
    parser.add_argument("--epochs", type=int, default=3)
    parser.add_argument("--batch_size", type=int, default=8)
    parser.add_argument("--lr", type=float, default=2e-5)
    parser.add_argument("--max_length", type=int, default=128)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    logger.info("Starting training: %s", args)
    set_global_seed(args.seed)

    dataset = load_and_prepare_dataset(args.data)
    ds = dataset.train_test_split(test_size=0.2, seed=args.seed)

    tokenizer = AutoTokenizer.from_pretrained(args.model_name)

    def tokenize_fn(examples):
        return tokenizer(
            examples["text"],
            padding="max_length",
            truncation=True,
            max_length=args.max_length,
        )

    ds = ds.map(tokenize_fn, batched=True)
    keep_cols = ("input_ids", "attention_mask", "token_type_ids", "label")
    ds = ds.remove_columns([c for c in ds["train"].column_names if c not in keep_cols])
    ds["train"] = ds["train"].with_format("torch")
    ds["test"] = ds["test"].with_format("torch")

    model = AutoModelForSequenceClassification.from_pretrained(
        args.model_name, num_labels=2
    )

    training_args = TrainingArguments(
        output_dir=args.output_dir,
        evaluation_strategy="epoch",
        save_strategy="epoch",
        learning_rate=args.lr,
        per_device_train_batch_size=args.batch_size,
        per_device_eval_batch_size=args.batch_size,
        num_train_epochs=args.epochs,
        weight_decay=0.01,
        load_best_model_at_end=True,
        metric_for_best_model="f1",
        logging_dir=os.path.join(args.output_dir, "logs"),
    )

    trainer = Trainer(
        model=model,
        args=training_args,
        train_dataset=ds["train"],
        eval_dataset=ds["test"],
        data_collator=DataCollatorWithPadding(tokenizer),
        compute_metrics=compute_metrics,
    )

    trainer.train()
    trainer.save_model(args.output_dir)
    tokenizer.save_pretrained(args.output_dir)
    logger.info("Training finished — saved to %s", args.output_dir)


if __name__ == "__main__":
    main()
