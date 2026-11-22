"""归并键校验与无归并键待核明细测试。

场景依据：
- 归并规则指定列缺值时，缺键行不能被并到同一个空键上；
- 一条空值 + 一条缺列的输入一同提交，分别生成待核明细交给复核人；
- 只补可确认的一条（从原文件找到正确的键），重跑归并；
- 同一行重跑只参与一次有效归并；
- 调用审计按待核明细关联受影响来源行与处理结果，失败说明原因；
- 逐行比对前后有效结果、待核原因和原始引用。
"""

import os
import sys
import shutil
import tempfile

# 设置测试环境
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

# 与其它测试共用会话级 DATA_ROOT；单独运行本文件时落到独立临时目录
os.environ.setdefault('DATA_ROOT', tempfile.mkdtemp(prefix='merge_review_test_'))


def _reset_store():
    from app.seed import SEED_ROWS
    from app.store import store as s
    s._tables = {name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()}


def _clean_data_root():
    from app.services.data_manager import DATA_ROOT
    if os.path.exists(DATA_ROOT):
        shutil.rmtree(DATA_ROOT, ignore_errors=True)


def _add_rule(rule_id: int, status: str = "已生效", merge_key: str = "id",
              conflict: str = "主源优先") -> None:
    from app.store import store
    store.rows("data_merge").append({
        "id": rule_id,
        "规则编号": f"DATA-T{rule_id}",
        "主API": "用户API",
        "从API": "订单API",
        "归并键": merge_key,
        "冲突策略": conflict,
        "去重策略": "全保留",
        "结果排序": "",
        "规则版本": "v1",
        "status": status,
        "pending": status != "已生效",
        "abnormal": False,
    })


# 主侧：一条有效行 + 一条空值行；从侧：一条有效行 + 一条缺列行
SOURCE_A = [{"id": "1", "name": "张三"}, {"id": "", "name": "空键主源行"}]
SOURCE_B = [{"id": "2", "amount": 100}, {"amount": 200, "note": "缺列从源行"}]
ROW_REFS = {
    "primary": ["fileA:row:0", "fileA:row:1"],
    "secondary": ["fileB:row:0", "fileB:row:1"],
}


class TestMergeKeyValidation:
    """归并键校验与待核明细流转"""

    def setup_method(self):
        _reset_store()
        _clean_data_root()
        _add_rule(100)

    # ----------------------------------------------------------
    # 空键不并组 + 待核明细生成
    # ----------------------------------------------------------

    def test_empty_and_missing_key_rows_not_grouped_together(self):
        """空值与缺列行不再并到同一个空键上，分别生成待核明细"""
        from app.services.data_merge import DataMergeService

        svc = DataMergeService()
        result = svc.run_validated_merge(100, SOURCE_A, SOURCE_B, row_refs=ROW_REFS)
        assert result["ok"], "归并校验应成功执行"

        # 有效结果只含两条有效键行，空值没有被当成共同键
        assert result["counts"]["merged"] == 2
        assert sorted(str(r.get("id")) for r in result["merged"]) == ["1", "2"]
        assert all(str(r.get("id", "")).strip() for r in result["merged"])

        # 两条缺键行各自独立待核，没有并到同一空键组
        pendings = result["pending_reviews"]
        assert len(pendings) == 2
        by_reason = {p["reason"]: p for p in pendings}
        assert by_reason["empty_value"]["source_side"] == "primary"
        assert by_reason["missing_column"]["source_side"] == "secondary"
        assert by_reason["empty_value"]["pending_id"] != by_reason["missing_column"]["pending_id"]
        assert result["result"] == "部分可交付"

        # 旧接口同样不再把缺键行并到空键上（输入4行 -> 有效2行，无静默丢行/覆盖）
        legacy = svc.merge_sources(100, SOURCE_A, SOURCE_B)
        assert len(legacy) == 2
        assert all(str(r.get("id", "")).strip() for r in legacy)

    def test_pending_detail_keeps_source_identity(self):
        """待核明细保留原行身份与原始行快照，可沿引用准确回到原输入"""
        from app.services.data_merge import DataMergeService

        svc = DataMergeService()
        result = svc.run_validated_merge(100, SOURCE_A, SOURCE_B, row_refs=ROW_REFS)
        by_reason = {p["reason"]: p for p in result["pending_reviews"]}

        empty_p = by_reason["empty_value"]
        assert empty_p["row_ref"] == "fileA:row:1"
        assert empty_p["row_snapshot"] == {"id": "", "name": "空键主源行"}
        assert empty_p["merge_key"] == "id"
        assert empty_p["rule_no"] == "DATA-T100"
        assert empty_p["reason_label"] == "归并键值为空"
        assert empty_p["status"] == "待补键"

        missing_p = by_reason["missing_column"]
        assert missing_p["row_ref"] == "fileB:row:1"
        assert missing_p["row_snapshot"] == {"amount": 200, "note": "缺列从源行"}
        assert missing_p["reason_label"] == "归并键列缺失"

        # 明细持久化，复核人可按规则分侧检索
        stored = svc.list_pending_reviews(rule_id=100, status="待补键")
        assert len(stored) == 2
        assert {s["source_side"] for s in stored} == {"primary", "secondary"}

    # ----------------------------------------------------------
    # 补键 + 重试
    # ----------------------------------------------------------

    def test_complete_key_then_retry_recovers_row(self):
        """只补可确认的一条：补键重试后进入可交付，另一条仍待核"""
        from app.services.data_merge import DataMergeService

        svc = DataMergeService()
        result = svc.run_validated_merge(100, SOURCE_A, SOURCE_B, row_refs=ROW_REFS)
        by_reason = {p["reason"]: p for p in result["pending_reviews"]}
        empty_id = by_reason["empty_value"]["pending_id"]
        missing_id = by_reason["missing_column"]["pending_id"]

        # 复核人从原文件找到空值行的正确键并补录
        completed, msg = svc.complete_pending_key(empty_id, "7", reviewer="复核人甲")
        assert completed is not None, msg
        assert completed["status"] == "已补键"
        assert completed["completed_key"] == "7"
        assert completed["reviewer"] == "复核人甲"
        # 键写回原行快照，保持原行身份
        assert completed["row_snapshot"]["id"] == "7"
        assert completed["row_ref"] == "fileA:row:1"

        # 缺列那条尚未补键，不能重试
        rejected, msg = svc.retry_pending_merge(missing_id)
        assert rejected is None and "尚未补录" in msg

        # 重试补键行：进入有效结果
        retried, msg = svc.retry_pending_merge(empty_id)
        assert retried is not None and retried["ok"], msg
        assert retried["delivered"] is True
        assert retried["merged_row"]["id"] == "7"
        assert retried["merged_row"]["name"] == "空键主源行"
        assert retried["merged_row"]["_source"] == "primary"

        # 可交付确认：首次有效结果2行 + 补键重试1行；缺列行仍待核
        summary = svc.deliverable_summary(100)
        assert summary["deliverable_count"] == 3
        assert summary["pending_count"] == 1
        remaining = summary["pending_reviews"][0]
        assert remaining["pending_id"] == missing_id
        assert remaining["reason"] == "missing_column"
        assert remaining["status"] == "待补键"

    def test_complete_rejects_empty_key_and_unknown_pending(self):
        """补键校验：空键拒绝、明细不存在拒绝"""
        from app.services.data_merge import DataMergeService

        svc = DataMergeService()
        result = svc.run_validated_merge(100, SOURCE_A, SOURCE_B, row_refs=ROW_REFS)
        pid = result["pending_reviews"][0]["pending_id"]

        bad, msg = svc.complete_pending_key(pid, "   ")
        assert bad is None and "不能为空" in msg
        bad, msg = svc.complete_pending_key(99999, "1")
        assert bad is None and "不存在" in msg

    def test_retry_same_row_only_merges_once(self):
        """同一行重跑只参与一次有效归并：重复重试幂等，整批重跑跳过已归并行"""
        from app.services.data_merge import DataMergeService
        from app.services.data_manager import list_audit

        svc = DataMergeService()
        result = svc.run_validated_merge(100, SOURCE_A, SOURCE_B, row_refs=ROW_REFS)
        by_reason = {p["reason"]: p for p in result["pending_reviews"]}
        empty_id = by_reason["empty_value"]["pending_id"]
        missing_id = by_reason["missing_column"]["pending_id"]

        svc.complete_pending_key(empty_id, "7")
        first, _ = svc.retry_pending_merge(empty_id)
        assert first["ok"] and first["already_merged"] is False

        # 重复重试：幂等返回原结果，不重复归并、不重复写审计
        audits_before = len(list_audit(caller="data_merge"))
        again, msg = svc.retry_pending_merge(empty_id)
        assert again["already_merged"] is True
        assert again["merged_row"]["id"] == "7"
        assert len(list_audit(caller="data_merge")) == audits_before

        # 整批重跑（原始输入未变）：已归并行按原行身份跳过，不重复参与
        rerun = svc.run_validated_merge(100, SOURCE_A, SOURCE_B, row_refs=ROW_REFS)
        assert rerun["counts"]["skipped_resolved"] == 1
        assert rerun["skipped_resolved"] == ["fileA:row:1"]
        assert all(str(r.get("id")) != "7" for r in rerun["merged"])

        # 仍未补键的缺列行不重复建明细，只在原明细上累加出现次数
        assert rerun["counts"]["pending"] == 1
        repending = rerun["pending_reviews"][0]
        assert repending["pending_id"] == missing_id
        assert repending["seen_count"] == 2
        assert len(svc.list_pending_reviews(rule_id=100)) == 2

        # 可交付总量不变：重跑有效2行 + 已重试1行
        summary = svc.deliverable_summary(100)
        assert summary["deliverable_count"] == 3

    # ----------------------------------------------------------
    # 审计关联
    # ----------------------------------------------------------

    def test_audit_links_pending_to_source_rows_and_results(self):
        """调用审计按待核明细关联受影响来源行与处理结果"""
        from app.services.data_merge import DataMergeService
        from app.services.call_audit import CallAuditService
        from app.services.data_manager import read_audit

        svc = DataMergeService()
        result = svc.run_validated_merge(100, SOURCE_A, SOURCE_B, row_refs=ROW_REFS)
        by_reason = {p["reason"]: p for p in result["pending_reviews"]}
        empty_id = by_reason["empty_value"]["pending_id"]
        missing_id = by_reason["missing_column"]["pending_id"]

        # 归并校验审计：关联两条待核明细、受影响来源行、处理结果
        audit = read_audit(result["audit_id"])
        assert audit["event"] == "merge_validated"
        assert audit["caller"] == "data_merge"
        assert sorted(audit["pending_ids"]) == sorted([empty_id, missing_id])
        affected = {r["row_ref"]: r for r in audit["affected_rows"]}
        assert affected["fileA:row:1"]["reason_label"] == "归并键值为空"
        assert affected["fileA:row:1"]["source_side"] == "primary"
        assert affected["fileB:row:1"]["reason_label"] == "归并键列缺失"
        assert affected["fileB:row:1"]["source_side"] == "secondary"
        assert audit["merged_count"] == 2
        assert audit["result"] == "部分可交付"

        # 调用审计模块按待核明细关联查询
        audit_svc = CallAuditService()
        linked = audit_svc.audits_for_merge_pending(empty_id)
        assert [a.get("event") for a in linked] == ["merge_validated"]

        # 补键 + 重试后，该明细的审计链完整
        svc.complete_pending_key(empty_id, "7", reviewer="复核人甲")
        svc.retry_pending_merge(empty_id)
        events = [a.get("event") for a in audit_svc.audits_for_merge_pending(empty_id)]
        assert events == ["merge_validated", "merge_key_completed", "merge_retry_completed"]
        retry_audit = [a for a in audit_svc.audits_for_merge_pending(empty_id)
                       if a.get("event") == "merge_retry_completed"][0]
        assert retry_audit["completed_key"] == "7"
        assert retry_audit["delivered"] is True
        assert retry_audit["affected_rows"][0]["row_ref"] == "fileA:row:1"

        # 缺列那条只有归并校验一条审计，原因可查
        events2 = [a.get("event") for a in audit_svc.audits_for_merge_pending(missing_id)]
        assert events2 == ["merge_validated"]

    def test_merge_failure_audit_explains_reason(self):
        """规则未生效导致归并失败时，审计说明原因"""
        from app.services.data_merge import DataMergeService
        from app.services.data_manager import read_audit

        _add_rule(101, status="草稿")
        svc = DataMergeService()
        result = svc.run_validated_merge(101, [{"id": "1"}], [{"id": "2"}])
        assert result["ok"] is False
        assert "未生效" in result["message"] or "不存在" in result["message"]

        audit = read_audit(result["audit_id"])
        assert audit["event"] == "merge_failed"
        assert audit["result"] == "归并失败"
        assert "未生效" in audit["reason"] or "不存在" in audit["reason"]

    # ----------------------------------------------------------
    # 任务场景：逐行比对前后
    # ----------------------------------------------------------

    def test_row_by_row_before_after_comparison(self):
        """一条空值 + 一条缺列一同提交，只补可确认的一条，逐行比对前后"""
        from app.services.data_merge import DataMergeService

        svc = DataMergeService()

        # ---- 前：一同提交 ----
        run1 = svc.run_validated_merge(100, SOURCE_A, SOURCE_B, row_refs=ROW_REFS)
        before_keys = sorted(str(r.get("id")) for r in run1["merged"])
        assert before_keys == ["1", "2"], "缺键行不得进入有效结果"
        before_pending = {p["row_ref"]: p for p in run1["pending_reviews"]}
        assert before_pending["fileA:row:1"]["reason_label"] == "归并键值为空"
        assert before_pending["fileB:row:1"]["reason_label"] == "归并键列缺失"
        assert before_pending["fileA:row:1"]["row_snapshot"]["name"] == "空键主源行"
        assert before_pending["fileB:row:1"]["row_snapshot"]["note"] == "缺列从源行"

        # ---- 只补可确认的一条（空值行从原文件找到正确键 7） ----
        empty_id = before_pending["fileA:row:1"]["pending_id"]
        svc.complete_pending_key(empty_id, "7", reviewer="复核人甲")
        retried, _ = svc.retry_pending_merge(empty_id)
        assert retried["delivered"] is True

        # ---- 后：逐行比对 ----
        after = svc.deliverable_summary(100)
        after_rows = after["merged_rows"] + after["retried_rows"]
        after_keys = sorted(str(r.get("id")) for r in after_rows)
        assert after_keys == ["1", "2", "7"], "补键行应进入可交付，缺列行仍不得进入"

        # 补键行沿原行身份回到原输入，其他字段原样保留
        recovered = [r for r in after_rows if str(r.get("id")) == "7"][0]
        assert recovered["name"] == "空键主源行"
        assert recovered["_source"] == "primary"

        # 缺列行仍待核，原因与原始引用不变
        assert after["pending_count"] == 1
        still = after["pending_reviews"][0]
        assert still["row_ref"] == "fileB:row:1"
        assert still["reason_label"] == "归并键列缺失"
        assert still["row_snapshot"] == {"amount": 200, "note": "缺列从源行"}
        assert still["status"] == "待补键"

    # ----------------------------------------------------------
    # 导出记录归并校验
    # ----------------------------------------------------------

    def test_merge_from_exports_validated_flow(self):
        """主从导出记录归并校验：待核明细的原行身份指回导出文件行号"""
        from app.services.data_merge import DataMergeService
        from app.services.data_manager import write_export

        # 主源导出：一行空值；从源导出：整份缺归并键列
        write_export(1, {
            "export_id": 1, "task_no": "EXP-T1", "api_name": "用户API",
            "columns": ["id", "name"],
            "rows": [["1", "张三"], ["", "空键主源行"]],
            "_created_at": "2026-11-22T10:00:02+08:00",
        })
        write_export(2, {
            "export_id": 2, "task_no": "EXP-T2", "api_name": "订单API",
            "columns": ["amount", "note"],
            "rows": [["200", "缺列从源行"]],
            "_created_at": "2026-11-22T10:00:01+08:00",
        })

        svc = DataMergeService()
        result = svc.merge_from_exports(100)
        assert result["ok"] and result["run_id"], "已生效规则下导出归并应走校验流程"

        # 有效结果只有主源 id=1 一行
        assert [str(r.get("id")) for r in result["merged"]] == ["1"]

        # 两条待核明细的原行身份指回导出文件与行号
        by_ref = {p["row_ref"]: p for p in result["pending_reviews"]}
        assert by_ref["export:1:primary:row:1"]["reason"] == "empty_value"
        assert by_ref["export:2:secondary:row:0"]["reason"] == "missing_column"

        # 复核人从原导出文件找到空值行的键，补录后重试交付
        empty_id = by_ref["export:1:primary:row:1"]["pending_id"]
        svc.complete_pending_key(empty_id, "9", reviewer="复核人乙")
        retried, _ = svc.retry_pending_merge(empty_id)
        assert retried["delivered"] is True
        assert retried["merged_row"]["id"] == "9"

        summary = svc.deliverable_summary(100)
        assert summary["deliverable_count"] == 2
        assert summary["pending_count"] == 1

    def test_merge_from_exports_legacy_when_rule_inactive(self):
        """规则未生效时导出归并保持旧拼接行为，向后兼容"""
        from app.services.data_merge import DataMergeService
        from app.services.data_manager import write_export

        write_export(1, {
            "export_id": 1, "api_name": "用户API",
            "columns": ["id"], "rows": [["1"]],
            "_created_at": "2026-11-22T10:00:02+08:00",
        })
        write_export(2, {
            "export_id": 2, "api_name": "订单API",
            "columns": ["id"], "rows": [["2"]],
            "_created_at": "2026-11-22T10:00:01+08:00",
        })

        svc = DataMergeService()
        legacy = svc.merge_from_exports(999)
        assert isinstance(legacy, list) and len(legacy) == 2
