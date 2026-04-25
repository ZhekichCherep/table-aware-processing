import kagglehub
import pandas as pd
import os
import json
from pathlib import Path

# 1. Список релевантных датасетов (owner/name)
DATASETS = [
    "ucimlrepo/credit-card-customers",
    "mohammadrahimzadeh/banking-dataset-marketing-targets",
    "jorijnsmit/fundamentals-of-corporate-finance"
]

OUTPUT_DIR = Path("tests/kaggle_samples")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def process_dataset(ds_id: str):
    print(f"⬇️ Скачиваю {ds_id}...")
    path = kagglehub.dataset_download(ds_id)

    for root, _, files in os.walk(path):
        for f in files:
            if f.endswith(('.csv', '.xlsx', '.parquet')):
                fpath = os.path.join(root, f)
                print(f"📊 Обрабатываю {f}")
                try:
                    if f.endswith('.csv'):
                        df = pd.read_csv(fpath)
                    elif f.endswith('.xlsx'):
                        df = pd.read_excel(fpath)
                    elif f.endswith('.parquet'):
                        df = pd.read_parquet(fpath)

                    # Здесь подставляете вызов вашего модуля:
                    # chunks = your_chunker.run(df)
                    # meta = your_profiler.analyze(df)

                    # Для демо просто сохраняем статистику
                    report = {
                        "dataset": ds_id,
                        "file": f,
                        "shape": df.shape,
                        "dtypes": {c: str(t) for c, t in df.dtypes.items()},
                        "missing_pct": float(df.isnull().mean().mean() * 100)
                    }
                    out_file = OUTPUT_DIR / f"{ds_id.replace('/', '_')}_{f}.json"
                    out_file.write_text(json.dumps(report, indent=2, ensure_ascii=False))
                    print(f"✅ Сохранено в {out_file}")

                except Exception as e:
                    print(f"❌ Ошибка при чтении {f}: {e}")


if __name__ == "__main__":
    for ds in DATASETS:
        process_dataset(ds)
    print("🏁 Готово. Проверьте папку tests/kaggle_samples/")