#!/usr/bin/env python3
"""
Clean, single-copy training script (no duplicates).

This file trains a BERT classifier like the project's notebook.
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
from sklearn.metrics import (
    accuracy_score,
    precision_recall_fscore_support,
)
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
    precision, recall, f1, _ = (
        precision_recall_fscore_support(
            labels, preds, average="binary", zero_division=0
        )
    )
    acc = accuracy_score(labels, preds)
    return {
        "accuracy": acc,
        "precision": precision,
        "recall": recall,
        "f1": f1,
    }


def set_global_seed(seed: int) -> None:
    random.seed(seed)
    np.random.seed(seed)
    if torch is not None:
        torch.manual_seed(seed)
        if torch.cuda.is_available():
            torch.cuda.manual_seed_all(seed)
    set_seed(seed)


def load_and_prepare_dataset(
    path: str,
    text_col: str = "Objeto",
    label_col: str = "Relevante",
) -> HfDataset:
    df = pd.read_csv(path)
    if text_col in df.columns and label_col in df.columns:
        df = df[[text_col, label_col]].rename(
            columns={text_col: "text", label_col: "label"}
        )
    elif "text" in df.columns and "label" in df.columns:
        df = df[["text", "label"]]
    else:
        raise ValueError("Dataset must contain text/label or Objeto/Relevante")
    df["label"] = df["label"].astype(str).str.strip().str.replace(r"\s+", "", regex=True)
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
        return tokenizer(examples["text"], padding="max_length", truncation=True, max_length=args.max_length)

    ds = ds.map(tokenize_fn, batched=True)
    keep_cols = ("input_ids", "attention_mask", "token_type_ids", "label")
    ds = ds.remove_columns([c for c in ds["train"].column_names if c not in keep_cols])
    ds["train"] = ds["train"].with_format("torch")
    ds["test"] = ds["test"].with_format("torch")

    model = AutoModelForSequenceClassification.from_pretrained(args.model_name, num_labels=2)

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
#!/usr/bin/env python3
"""
train.py

Minimal reproducible training script that mirrors `classificador-text.ipynb`.

Usage (example):
    python train.py --data data/treinamento/2026-03-14/dataset.csv \
        --model_name neuralmind/bert-base-portuguese-cased \
        --output_dir modelo_classificador --epochs 3 --batch_size 8

This script uses Hugging Face Transformers Trainer + Datasets.
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
from sklearn.metrics import (
    accuracy_score,
    precision_recall_fscore_support,
)
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
    precision, recall, f1, _ = (
        precision_recall_fscore_support(
            labels, preds, average="binary", zero_division=0
        )
    )
    acc = accuracy_score(labels, preds)
    return {
        "accuracy": acc,
        "precision": precision,
        "recall": recall,
        "f1": f1,
    }


def set_global_seed(seed: int) -> None:
    random.seed(seed)
    np.random.seed(seed)
    if torch is not None:
        torch.manual_seed(seed)
        if torch.cuda.is_available():
            torch.cuda.manual_seed_all(seed)
    set_seed(seed)


def load_and_prepare_dataset(
    path: str,
    text_col: str = "Objeto",
    label_col: str = "Relevante",
) -> HfDataset:
    df = pd.read_csv(path)

    # Normalize expected column names used in the notebook
    if text_col in df.columns and label_col in df.columns:
        df = df[[text_col, label_col]].rename(
            columns={text_col: "text", label_col: "label"}
        )
    elif "text" in df.columns and "label" in df.columns:
        df = df[["text", "label"]]
    else:
        raise ValueError(
            f"Dataset must contain columns {text_col}/{label_col} or text/label"
        )

    # sanitize labels like in notebook: keep '0' or '1'
    df["label"] = (
        df["label"].astype(str)
        .str.strip()
        .str.replace(r"\s+", "", regex=True)
    )
    df = df[df["label"].isin(["0", "1"])].copy()
    df["label"] = df["label"].astype(int)

    df["text"] = df["text"].astype(str).str.strip()
    df = df[df["text"] != ""].copy()
    df = df.reset_index(drop=True)

    return HfDataset.from_pandas(df)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Fine-tune a Hugging Face Transformer for binary "
            "classification"
        )
    )
    parser.add_argument("--data", required=True, help="Path to CSV dataset")
    parser.add_argument(
        "--model_name",
        default=("neuralmind/bert-base-portuguese-cased"),
    )
    parser.add_argument("--output_dir", default="modelo_classificador")
    parser.add_argument("--epochs", type=int, default=3)
    parser.add_argument("--batch_size", type=int, default=8)
    parser.add_argument("--lr", type=float, default=2e-5)
    parser.add_argument("--max_length", type=int, default=128)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--text_col", type=str, default="Objeto")
    parser.add_argument("--label_col", type=str, default="Relevante")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    logger.info("Training with args: %s", args)

    set_global_seed(args.seed)

    # load dataset and split
    dataset = load_and_prepare_dataset(
        args.data, text_col=args.text_col, label_col=args.label_col
    )
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
    keep_cols = (
        "input_ids",
        "attention_mask",
        "token_type_ids",
        "label",
    )
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

    data_collator = DataCollatorWithPadding(tokenizer)

    trainer = Trainer(
        model=model,
        args=training_args,
        train_dataset=ds["train"],
        eval_dataset=ds["test"],
        data_collator=data_collator,
        compute_metrics=compute_metrics,
    )

    trainer.train()

    trainer.save_model(args.output_dir)
    tokenizer.save_pretrained(args.output_dir)

    logger.info("Training finished — saved to %s", args.output_dir)


if __name__ == "__main__":
    main()
    python train.py --data data/treinamento/2026-03-14/dataset.csv \
        --model_name neuralmind/bert-base-portuguese-cased \
        --output_dir modelo_classificador --epochs 3 --batch_size 8

This script uses Hugging Face Transformers Trainer + Datasets.
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
from sklearn.metrics import (
    accuracy_score,
    precision_recall_fscore_support,
)
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
    return {
        "accuracy": acc,
        "precision": precision,
        "recall": recall,
        "f1": f1,
    }


def set_global_seed(seed: int) -> None:
    random.seed(seed)
    np.random.seed(seed)
    if torch is not None:
        torch.manual_seed(seed)
        if torch.cuda.is_available():
            torch.cuda.manual_seed_all(seed)
    set_seed(seed)


def load_and_prepare_dataset(
    path: str,
    text_col: str = "Objeto",
    label_col: str = "Relevante",
) -> HfDataset:
    df = pd.read_csv(path)

    # Normalize expected column names used in the notebook
    if text_col in df.columns and label_col in df.columns:
        df = df[[text_col, label_col]].rename(
            columns={text_col: "text", label_col: "label"}
        )
    elif "text" in df.columns and "label" in df.columns:
        df = df[["text", "label"]]
    else:
        raise ValueError(
            f"Dataset must contain columns {text_col}/{label_col} or text/label"
        )

    # sanitize labels like in notebook: keep '0' or '1'
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
    parser = argparse.ArgumentParser(
        description=(
            "Fine-tune a Hugging Face Transformer for binary " "classification"
        )
    )
    parser.add_argument("--data", required=True, help="Path to CSV dataset")
    parser.add_argument(
        "--model_name",
        default=("neuralmind/bert-base-portuguese-cased"),
    )
    parser.add_argument("--output_dir", default="modelo_classificador")
    parser.add_argument("--epochs", type=int, default=3)
    parser.add_argument("--batch_size", type=int, default=8)
    parser.add_argument("--lr", type=float, default=2e-5)
    parser.add_argument("--max_length", type=int, default=128)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--text_col", type=str, default="Objeto")
    parser.add_argument("--label_col", type=str, default="Relevante")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    logger.info("Training with args: %s", args)

    set_global_seed(args.seed)

    # load dataset and split
    dataset = load_and_prepare_dataset(
        args.data, text_col=args.text_col, label_col=args.label_col
    )
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
    keep_cols = (
        "input_ids",
        "attention_mask",
        "token_type_ids",
        "label",
    )
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

    data_collator = DataCollatorWithPadding(tokenizer)

    trainer = Trainer(
        model=model,
        args=training_args,
        train_dataset=ds["train"],
        eval_dataset=ds["test"],
        data_collator=data_collator,
        compute_metrics=compute_metrics,
    )

    trainer.train()

    trainer.save_model(args.output_dir)
    tokenizer.save_pretrained(args.output_dir)

    logger.info("Training finished — saved to %s", args.output_dir)


if __name__ == "__main__":
    main()
#!/usr/bin/env python3
"""
train.py

Minimal reproducible training script that mirrors `classificador-text.ipynb`.

Usage (example):
    python train.py --data data/treinamento/2026-03-14/dataset.csv \
        --model_name neuralmind/bert-base-portuguese-cased \
        --output_dir modelo_classificador --epochs 3 --batch_size 8

This script uses Hugging Face Transformers Trainer + Datasets.
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


def load_and_prepare_dataset(
    path: str, text_col: str = "Objeto", label_col: str = "Relevante"
) -> HfDataset:
    df = pd.read_csv(path)

    # Normalize expected column names used in the notebook
    if text_col in df.columns and label_col in df.columns:
        df = df[[text_col, label_col]].rename(
            columns={text_col: "text", label_col: "label"}
        )
    elif "text" in df.columns and "label" in df.columns:
        df = df[["text", "label"]]
    else:
        raise ValueError(
            f"Dataset must contain columns {text_col}/{label_col} or text/label"
        )

    # sanitize labels like in notebook: keep '0' or '1'
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
    parser = argparse.ArgumentParser(
        description="Fine-tune a Hugging Face Transformer for binary classification"
    )
    parser.add_argument("--data", required=True, help="Path to CSV dataset")
    parser.add_argument("--model_name", default="neuralmind/bert-base-portuguese-cased")
    parser.add_argument("--output_dir", default="modelo_classificador")
    parser.add_argument("--epochs", type=int, default=3)
    parser.add_argument("--batch_size", type=int, default=8)
    parser.add_argument("--lr", type=float, default=2e-5)
    parser.add_argument("--max_length", type=int, default=128)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--text_col", type=str, default="Objeto")
    parser.add_argument("--label_col", type=str, default="Relevante")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    logger.info("Training with args: %s", args)

    set_global_seed(args.seed)

    # load dataset and split
    dataset = load_and_prepare_dataset(
        args.data, text_col=args.text_col, label_col=args.label_col
    )
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
    ds = ds.remove_columns(
        [
            c
            for c in ds["train"].column_names
            if c not in ("input_ids", "attention_mask", "token_type_ids", "label")
        ]
    )

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

    data_collator = DataCollatorWithPadding(tokenizer)

    trainer = Trainer(
        model=model,
        args=training_args,
        train_dataset=ds["train"],
        eval_dataset=ds["test"],
        data_collator=data_collator,
        compute_metrics=compute_metrics,
    )

    trainer.train()

    trainer.save_model(args.output_dir)
    tokenizer.save_pretrained(args.output_dir)

    logger.info("Training finished — model and tokenizer saved to %s", args.output_dir)


if __name__ == "__main__":
    main()
