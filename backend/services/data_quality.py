# backend/services/data_quality.py
from typing import Dict, List
import pandas as pd
from datetime import datetime


class DataQualityChecker:
    """数据质量检查器"""

    def __init__(self):
        self.checks = {
            "completeness": self.check_completeness,
            "consistency": self.check_consistency,
            "uniqueness": self.check_uniqueness,
            "accuracy": self.check_accuracy
        }

    def profile_dataframe(self, df: pd.DataFrame) -> Dict:
        """数据画像分析"""
        profile = {
            "row_count": len(df),
            "column_count": len(df.columns),
            "data_types": df.dtypes.to_dict(),
            "basic_stats": {},
            "quality_metrics": {}
        }

        # 基本统计信息
        for column in df.columns:
            profile["basic_stats"][column] = {
                "non_null_count": df[column].count(),
                "null_count": df[column].isnull().sum(),
                "unique_count": df[column].nunique(),
                "sample_values": df[column].dropna().head(3).tolist()
            }

        # 质量指标
        profile["quality_metrics"] = self.run_quality_checks(df)

        return profile

    def run_quality_checks(self, df: pd.DataFrame) -> Dict:
        """运行数据质量检查"""
        metrics = {}

        for check_name, check_func in self.checks.items():
            try:
                metrics[check_name] = check_func(df)
            except Exception as e:
                metrics[check_name] = {"error": str(e)}

        # 计算总体质量分数
        metrics["overall_score"] = self._calculate_overall_score(metrics)

        return metrics

    def check_completeness(self, df: pd.DataFrame) -> Dict:
        """完整性检查"""
        completeness = {}
        total_rows = len(df)

        for column in df.columns:
            non_null_count = df[column].count()
            completeness_rate = (non_null_count / total_rows) * 100
            completeness[column] = {
                "completeness_rate": round(completeness_rate, 2),
                "missing_count": total_rows - non_null_count,
                "status": "good" if completeness_rate >= 95 else "poor"
            }

        return completeness

    def check_consistency(self, df: pd.DataFrame) -> Dict:
        """一致性检查"""
        consistency = {}

        for column in df.columns:
            # 检查数据类型一致性
            dtype = str(df[column].dtype)
            consistency[column] = {
                "data_type": dtype,
                "inconsistent_types": 0,  # 简化处理
                "status": "consistent"
            }

        return consistency

    def check_uniqueness(self, df: pd.DataFrame) -> Dict:
        """唯一性检查"""
        uniqueness = {}

        for column in df.columns:
            unique_count = df[column].nunique()
            total_count = df[column].count()
            uniqueness_rate = (unique_count / total_count) * 100 if total_count > 0 else 0

            uniqueness[column] = {
                "unique_count": unique_count,
                "uniqueness_rate": round(uniqueness_rate, 2),
                "duplicate_count": total_count - unique_count,
                "status": "unique" if uniqueness_rate > 99 else "has_duplicates"
            }

        return uniqueness

    def check_accuracy(self, df: pd.DataFrame) -> Dict:
        """准确性检查（基础版本）"""
        accuracy = {}

        for column in df.columns:
            # 基础准确性检查 - 可根据具体业务规则扩展
            accuracy[column] = {
                "valid_patterns": 0,  # 可添加正则验证
                "out_of_range": 0,  # 可添加范围验证
                "status": "needs_validation"
            }

        return accuracy

    def _calculate_overall_score(self, metrics: Dict) -> float:
        """计算总体质量分数"""
        total_score = 0
        weight = {
            "completeness": 0.4,
            "consistency": 0.3,
            "uniqueness": 0.2,
            "accuracy": 0.1
        }

        for check_name, results in metrics.items():
            if check_name in weight and isinstance(results, dict):
                # 简化评分逻辑
                column_scores = []
                for col_result in results.values():
                    if isinstance(col_result, dict) and "status" in col_result:
                        score = 100 if col_result["status"] in ["good", "consistent", "unique"] else 50
                        column_scores.append(score)

                if column_scores:
                    avg_score = sum(column_scores) / len(column_scores)
                    total_score += avg_score * weight[check_name]

        return round(total_score, 2)


def generate_quality_report(asset_id: str, df: pd.DataFrame) -> Dict:
    """生成数据质量报告"""
    checker = DataQualityChecker()
    profile = checker.profile_dataframe(df)

    report = {
        "asset_id": asset_id,
        "generated_time": datetime.now().isoformat(),
        "summary": {
            "overall_score": profile["quality_metrics"]["overall_score"],
            "row_count": profile["row_count"],
            "data_volume": f"{df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB"
        },
        "detailed_metrics": profile["quality_metrics"],
        "recommendations": generate_quality_recommendations(profile["quality_metrics"])
    }

    return report


def generate_quality_recommendations(metrics: Dict) -> List[str]:
    """生成质量改进建议"""
    recommendations = []

    completeness = metrics.get("completeness", {})
    for column, stats in completeness.items():
        if isinstance(stats, dict) and stats.get("completeness_rate", 100) < 95:
            recommendations.append(f"列 '{column}' 缺失值较多({100 - stats['completeness_rate']}%)，建议检查数据源")

    uniqueness = metrics.get("uniqueness", {})
    for column, stats in uniqueness.items():
        if isinstance(stats, dict) and stats.get("uniqueness_rate", 100) < 80:
            recommendations.append(f"列 '{column}' 重复值较多，建议数据清洗")

    if metrics.get("overall_score", 0) < 80:
        recommendations.append("整体数据质量有待提升，建议进行全面的数据治理")

    return recommendations