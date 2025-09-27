# backend/services/policy_engine.py
import re
from typing import List, Dict
from datetime import datetime


class PolicyEngine:
    """策略引擎 - 自动数据分类和标记"""

    def __init__(self):
        self.pii_patterns = {
            'email': r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
            'phone': r'\b(\+?1?[-.\s]?\(?[0-9]{3}\)?[-.\s]?[0-9]{3}[-.\s]?[0-9]{4})\b',
            'ssn': r'\b\d{3}-\d{2}-\d{4}\b',
            'credit_card': r'\b\d{4}[- ]?\d{4}[- ]?\d{4}[- ]?\d{4}\b'
        }

        self.sensitive_keywords = {
            'password', 'pwd', 'secret', 'token', 'key', 'auth',
            'salary', 'income', 'credit', 'bank', 'account'
        }

    def analyze_asset(self, asset_data: Dict) -> Dict:
        """分析资产并应用策略"""
        analysis_result = {
            "is_pii": False,
            "pii_types": [],
            "sensitivity_level": "low",
            "recommended_tags": [],
            "compliance_risks": []
        }

        # 分析资产名称和描述
        text_to_analyze = f"{asset_data.get('name', '')} {asset_data.get('description', '')}".lower()

        # 检查PII模式
        analysis_result["pii_types"] = self._detect_pii_patterns(text_to_analyze)
        analysis_result["is_pii"] = len(analysis_result["pii_types"]) > 0

        # 检查敏感关键词
        sensitive_found = self._detect_sensitive_keywords(text_to_analyze)
        if sensitive_found:
            analysis_result["recommended_tags"].extend(sensitive_found)

        # 确定敏感级别
        analysis_result["sensitivity_level"] = self._determine_sensitivity_level(
            analysis_result["pii_types"],
            sensitive_found
        )

        # 生成合规风险建议
        analysis_result["compliance_risks"] = self._assess_compliance_risks(analysis_result)

        return analysis_result

    def _detect_pii_patterns(self, text: str) -> List[str]:
        """检测PII（个人身份信息）模式"""
        detected_types = []

        for pii_type, pattern in self.pii_patterns.items():
            if re.search(pattern, text, re.IGNORECASE):
                detected_types.append(pii_type)

        return detected_types

    def _detect_sensitive_keywords(self, text: str) -> List[str]:
        """检测敏感关键词"""
        detected_keywords = []

        for keyword in self.sensitive_keywords:
            if keyword.lower() in text.lower():
                detected_keywords.append(keyword)

        return detected_keywords

    def _determine_sensitivity_level(self, pii_types: List[str], sensitive_keywords: List[str]) -> str:
        """确定敏感级别"""
        if pii_types:
            return "high"
        elif sensitive_keywords:
            return "medium"
        else:
            return "low"

    def _assess_compliance_risks(self, analysis_result: Dict) -> List[str]:
        """评估合规风险"""
        risks = []

        if analysis_result["is_pii"]:
            risks.append("包含个人身份信息(PII)，需要数据脱敏")
            risks.append("可能受GDPR/数据安全法规范")

        if analysis_result["sensitivity_level"] == "high":
            risks.append("高敏感数据，需要严格访问控制")

        return risks

    def generate_data_governance_policy(self, asset_data: Dict) -> Dict:
        """生成数据治理策略"""
        analysis = self.analyze_asset(asset_data)

        policy = {
            "asset_id": asset_data.get("id"),
            "asset_name": asset_data.get("name"),
            "analysis_time": datetime.now().isoformat(),
            "sensitivity_level": analysis["sensitivity_level"],
            "access_control_recommendations": self._generate_access_control_recommendations(analysis),
            "data_retention_recommendation": self._generate_retention_recommendation(analysis),
            "encryption_requirements": self._generate_encryption_requirements(analysis),
            "monitoring_requirements": self._generate_monitoring_requirements(analysis)
        }

        return policy

    def _generate_access_control_recommendations(self, analysis: Dict) -> List[str]:
        """生成访问控制建议"""
        recommendations = []

        if analysis["sensitivity_level"] == "high":
            recommendations.extend([
                "实施基于角色的访问控制(RBAC)",
                "要求多因素认证(MFA)",
                "记录所有数据访问日志",
                "定期进行访问权限审查"
            ])
        elif analysis["sensitivity_level"] == "medium":
            recommendations.extend([
                "实施基本的访问控制",
                "记录关键数据访问操作"
            ])

        return recommendations

    def _generate_retention_recommendation(self, analysis: Dict) -> str:
        """生成数据保留建议"""
        if analysis["is_pii"]:
            return "根据法规要求，最长保留2年"
        else:
            return "根据业务需要，可保留3-5年"

    def _generate_encryption_requirements(self, analysis: Dict) -> List[str]:
        """生成加密要求"""
        requirements = []

        if analysis["sensitivity_level"] in ["high", "medium"]:
            requirements.append("传输过程中使用TLS加密")
            requirements.append("静态数据使用AES-256加密")

        return requirements

    def _generate_monitoring_requirements(self, analysis: Dict) -> List[str]:
        """生成监控要求"""
        requirements = []

        if analysis["sensitivity_level"] == "high":
            requirements.extend([
                "实时监控异常访问模式",
                "设置数据泄露检测告警",
                "每周生成安全审计报告"
            ])

        return requirements


# 集成到GraphService的扩展
class EnhancedGraphService:
    """增强的图服务，集成策略引擎"""

    def __init__(self, uri, user, password):
        from backend.services.graph_service import GraphService
        self.graph_service = GraphService(uri, user, password)
        self.policy_engine = PolicyEngine()

    def create_asset_with_policy(self, asset_data):
        """创建资产并自动应用策略"""
        # 创建资产
        self.graph_service.create_asset(asset_data)

        # 应用策略分析
        policy = self.policy_engine.generate_data_governance_policy({
            "id": asset_data.id,
            "name": asset_data.name,
            "description": asset_data.description,
            "type": asset_data.type
        })

        # 存储策略结果（这里简化处理，实际应该存储到数据库）
        print(f"策略分析完成: {policy}")

        return policy