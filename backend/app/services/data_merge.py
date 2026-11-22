"""数据归并领域服务：管理多API数据源的归并、冲突处理和去重策略。

跨模块数据流：
- 消费 api_registry 快照 -> 归并源API核验
- 消费 data_filter 快照 -> 各源数据归并前先过滤
- 被 export_task 消费 -> 归并后数据导出
- 归并结果写入共享快照
- 无归并键待核明细写入 data/merge_review/ -> 复核人补键后重试归并
- 归并运行记录写入 data/merge_runs/ -> 可交付确认
- 归并/补键/重试均写 call_audit 审计，按待核明细关联受影响来源行与处理结果

归并键校验约定：
- 归并键列缺失或值为空的行不参与归并，空值不会被当成两个来源的有效共同键；
- 缺键行生成无归并键待核明细，保留原行身份（row_ref）与原始行快照，
  补齐键后沿待核明细准确回到原输入；
- 同一行重跑时只参与一次有效归并（待核明细状态机 + 已归并行身份跳过）。
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class DataMergeService(BaseModuleService):
    MODULE = "data_merge"
    ENTITY = "归并规则"
    OPERATION_TERM = "去重"
    LIST_FIELDS = ["规则编号", "主API", "从API", "归并键", "冲突策略", "去重策略", "结果排序", "规则版本"]
    REQUIRED_FIELDS = ["规则编号", "主API", "从API"]
    STATUS_ORDER = ["草稿", "待校验", "已生效", "已停用"]
    ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "停用规则": "已停用"}
    NEGATIVE_ACTIONS = ["停用规则"]

    # 无归并键待核原因
    REASON_MISSING_COLUMN = "missing_column"
    REASON_EMPTY_VALUE = "empty_value"
    REASON_LABELS = {
        REASON_MISSING_COLUMN: "归并键列缺失",
        REASON_EMPTY_VALUE: "归并键值为空",
    }
    # 待核明细状态机：待补键 -> 已补键 -> 已归并
    PENDING_OPEN_STATUSES = ("待补键", "已补键")
    PENDING_DONE_STATUS = "已归并"

    # ----------------------------------------------------------
    # 归并键校验
    # ----------------------------------------------------------

    @classmethod
    def _classify_row_key(cls, row: dict[str, Any], merge_key: str) -> tuple[str | None, str | None]:
        """检查单行的归并键，返回 (规范化键, 待核原因)；reason 为 None 表示键有效。"""
        if merge_key not in row:
            return None, cls.REASON_MISSING_COLUMN
        value = row.get(merge_key)
        key = str(value).strip() if value is not None else ""
        if not key:
            return None, cls.REASON_EMPTY_VALUE
        return key, None

    def _validate_side(self, rule: dict[str, Any], rows: list[dict[str, Any]],
                       side: str, refs: list[str] | None,
                       skip_refs: set[str]) -> tuple[list[tuple[str, dict[str, Any]]],
                                                     list[dict[str, Any]],
                                                     list[str]]:
        """校验一侧来源行，返回 (有效(键,行)列表, 待核明细列表, 被跳过的已归并行引用)。"""
        merge_key = rule.get("归并键", "id")
        pairs: list[tuple[str, dict[str, Any]]] = []
        pendings: list[dict[str, Any]] = []
        skipped: list[str] = []
        for idx, row in enumerate(rows):
            row_ref = refs[idx] if refs and idx < len(refs) else f"{side}#row{idx}"
            if row_ref in skip_refs:
                # 该原行已通过补键重试参与过有效归并，重跑时不再重复参与
                skipped.append(row_ref)
                continue
            key, reason = self._classify_row_key(row, merge_key)
            if reason is None:
                pairs.append((key, row))
            else:
                pendings.append({
                    "rule_id": rule.get("id"),
                    "rule_no": rule.get("规则编号", ""),
                    "merge_key": merge_key,
                    "source_side": side,
                    "reason": reason,
                    "reason_label": self.REASON_LABELS[reason],
                    "row_ref": row_ref,
                    "row_snapshot": dict(row),
                })
        return pairs, pendings, skipped

    # ----------------------------------------------------------
    # 归并执行
    # ----------------------------------------------------------

    def _merge_valid_pairs(self, rule: dict[str, Any],
                           pairs_a: list[tuple[str, dict[str, Any]]],
                           pairs_b: list[tuple[str, dict[str, Any]]]) -> list[dict[str, Any]]:
        """归已校验有效的 (键, 行) 对，应用冲突策略、去重策略与结果排序。"""
        merge_key = rule.get("归并键", "id")
        conflict_policy = rule.get("冲突策略", "主源优先")
        dedup_policy = rule.get("去重策略", "全保留")

        merged: dict[str, dict[str, Any]] = {}
        for key, row in pairs_a:
            merged[f"a_{key}"] = {"_source": "primary", **row}

        for key, row in pairs_b:
            pk = f"a_{key}"
            if pk in merged:
                if conflict_policy == "主源优先":
                    continue
                elif conflict_policy == "从源覆盖":
                    merged[pk] = {"_source": "secondary", **row}
                elif conflict_policy == "合并字段":
                    merged[pk].update({"from_secondary": row})
            else:
                merged[f"b_{key}"] = {"_source": "secondary", **row}

        result = list(merged.values())

        # 去重
        if dedup_policy == "按键去重":
            seen = set()
            deduped = []
            for row in result:
                key = str(row.get(merge_key, ""))
                if key not in seen:
                    seen.add(key)
                    deduped.append(row)
            result = deduped

        # 排序
        sort_field = rule.get("结果排序", "")
        if sort_field:
            result = sorted(result, key=lambda r: str(r.get(sort_field, "")))

        return result

    def merge_sources(self, rule_id: int, source_a_rows: list[dict[str, Any]],
                      source_b_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """按归并规则合并两个数据源。

        缺键行（归并键列缺失或值为空）不再并到同一个空键上，直接不进归并结果；
        需要引导补键的待核明细请使用 run_validated_merge。
        """
        rule = store.find(self.MODULE, rule_id)
        if rule is None or rule.get("status") != "已生效":
            return source_a_rows + source_b_rows

        pairs_a, _, _ = self._validate_side(rule, source_a_rows, "primary", None, set())
        pairs_b, _, _ = self._validate_side(rule, source_b_rows, "secondary", None, set())
        return self._merge_valid_pairs(rule, pairs_a, pairs_b)

    def run_validated_merge(self, rule_id: int, source_a_rows: list[dict[str, Any]],
                            source_b_rows: list[dict[str, Any]], *,
                            row_refs: dict[str, list[str]] | None = None,
                            note: str = "") -> dict[str, Any]:
        """带归并键校验的主从归并：有效行归并交付，缺键行转待核明细并写审计。

        row_refs: {"primary": [...], "secondary": [...]} 调用方提供的原行身份，
                  用于补齐键后沿待核明细准确回到原输入；缺省按本次提交序号生成。
        返回 {ok, run_id, merged, pending_reviews, skipped_resolved, counts, audit_id}。
        """
        from app.services.data_manager import (
            list_merge_reviews, next_merge_run_seq,
            write_merge_review, write_merge_run, next_merge_review_id,
        )

        rule = store.find(self.MODULE, rule_id)
        if rule is None or rule.get("status") != "已生效":
            message = "归并规则不存在或未生效，无法执行归并校验"
            audit_id = self._write_merge_audit({
                "api_name": rule.get("主API", "") if rule else "",
                "event": "merge_failed",
                "rule_id": rule_id,
                "rule_no": rule.get("规则编号", "") if rule else "",
                "reason": message,
                "result": "归并失败",
            })
            return {"ok": False, "message": message, "run_id": None, "merged": [],
                    "pending_reviews": [], "skipped_resolved": [], "counts": {},
                    "audit_id": audit_id}

        run_seq = next_merge_run_seq()
        run_id = f"MR-{run_seq:06d}"

        # 已归并的原行身份：重跑时跳过，保证同一行只参与一次有效归并
        resolved_refs = {
            r.get("row_ref")
            for r in list_merge_reviews(rule_id=rule_id, status=self.PENDING_DONE_STATUS)
            if r.get("row_ref")
        }

        refs = row_refs or {}
        pairs_a, pendings_a, skipped_a = self._validate_side(
            rule, source_a_rows, "primary", refs.get("primary"), resolved_refs)
        pairs_b, pendings_b, skipped_b = self._validate_side(
            rule, source_b_rows, "secondary", refs.get("secondary"), resolved_refs)

        merged = self._merge_valid_pairs(rule, pairs_a, pairs_b)

        # 持久化待核明细：同一规则同一原行身份的未结明细不重复创建，重跑只累加出现次数
        open_by_ref = {
            r.get("row_ref"): r
            for r in list_merge_reviews(rule_id=rule_id)
            if r.get("status") in self.PENDING_OPEN_STATUSES and r.get("row_ref")
        }
        pendings: list[dict[str, Any]] = []
        for item in [*pendings_a, *pendings_b]:
            existing = open_by_ref.get(item["row_ref"])
            if existing is not None:
                existing["seen_count"] = int(existing.get("seen_count", 1)) + 1
                existing["last_run_id"] = run_id
                write_merge_review(int(existing["pending_id"]), existing)
                item.update({
                    "pending_id": existing["pending_id"],
                    "status": existing["status"],
                    "seen_count": existing["seen_count"],
                    "duplicated": True,
                })
            else:
                pending_id = next_merge_review_id()
                item.update({
                    "pending_id": pending_id,
                    "status": "待补键",
                    "seen_count": 1,
                    "first_run_id": run_id,
                    "last_run_id": run_id,
                })
                write_merge_review(pending_id, item)
                open_by_ref[item["row_ref"]] = item
            pendings.append(item)

        counts = {
            "primary_total": len(source_a_rows),
            "secondary_total": len(source_b_rows),
            "valid_primary": len(pairs_a),
            "valid_secondary": len(pairs_b),
            "merged": len(merged),
            "pending": len(pendings),
            "skipped_resolved": len(skipped_a) + len(skipped_b),
        }
        result_label = "部分可交付" if pendings else "全部可交付"

        write_merge_run(run_seq, {
            "run_id": run_id,
            "rule_id": rule_id,
            "rule_no": rule.get("规则编号", ""),
            "note": note,
            "merged": merged,
            "pending_ids": [p["pending_id"] for p in pendings],
            "skipped_resolved_refs": skipped_a + skipped_b,
            "counts": counts,
            "result": result_label,
        })

        audit_id = self._write_merge_audit({
            "api_name": rule.get("主API", ""),
            "event": "merge_validated",
            "rule_id": rule_id,
            "rule_no": rule.get("规则编号", ""),
            "run_id": run_id,
            "primary_count": counts["primary_total"],
            "secondary_count": counts["secondary_total"],
            "merged_count": counts["merged"],
            "pending_count": counts["pending"],
            "pending_ids": [p["pending_id"] for p in pendings],
            "affected_rows": [
                {
                    "pending_id": p["pending_id"],
                    "row_ref": p["row_ref"],
                    "source_side": p["source_side"],
                    "reason": p["reason"],
                    "reason_label": p["reason_label"],
                }
                for p in pendings
            ],
            "skipped_resolved_refs": skipped_a + skipped_b,
            "result": result_label,
        })

        return {
            "ok": True,
            "run_id": run_id,
            "rule_id": rule_id,
            "merged": merged,
            "pending_reviews": pendings,
            "skipped_resolved": skipped_a + skipped_b,
            "counts": counts,
            "result": result_label,
            "audit_id": audit_id,
        }

    # ----------------------------------------------------------
    # 复核人补键与重试
    # ----------------------------------------------------------

    def complete_pending_key(self, pending_id: int, key_value: Any,
                             reviewer: str = "") -> tuple[dict[str, Any] | None, str]:
        """复核人为指定待核明细补录归并键，键值写回原行快照以保持原行身份。"""
        from app.services.data_manager import read_merge_review, write_merge_review

        pending = read_merge_review(pending_id)
        if not pending:
            return None, f"待核明细 {pending_id} 不存在"
        if pending.get("status") == self.PENDING_DONE_STATUS:
            return None, f"待核明细 {pending_id} 已归并，无需补键"
        key = str(key_value).strip() if key_value is not None else ""
        if not key:
            return None, "补录的归并键不能为空"

        pending["completed_key"] = key
        pending["reviewer"] = reviewer or "复核人"
        pending["status"] = "已补键"
        pending["completed_at"] = self._now_iso()
        snapshot = dict(pending.get("row_snapshot") or {})
        snapshot[pending.get("merge_key", "id")] = key
        pending["row_snapshot"] = snapshot
        write_merge_review(pending_id, pending)

        self._write_merge_audit({
            "api_name": "",
            "event": "merge_key_completed",
            "rule_id": pending.get("rule_id"),
            "rule_no": pending.get("rule_no", ""),
            "pending_id": pending_id,
            "affected_rows": [{
                "pending_id": pending_id,
                "row_ref": pending.get("row_ref"),
                "source_side": pending.get("source_side"),
                "reason": pending.get("reason"),
                "reason_label": pending.get("reason_label"),
            }],
            "completed_key": key,
            "reviewer": pending["reviewer"],
            "result": "已补键",
        })
        return pending, f"待核明细 {pending_id} 已补录归并键"

    def retry_pending_merge(self, pending_id: int,
                            counterpart_rows: list[dict[str, Any]] | None = None
                            ) -> tuple[dict[str, Any] | None, str]:
        """按原行身份重试归并：补键后的行只参与一次有效归并，重复重试幂等返回。

        counterpart_rows: 可选的对侧来源行，用于按冲突策略判定键冲突。
        """
        from app.services.data_manager import read_merge_review, write_merge_review

        pending = read_merge_review(pending_id)
        if not pending:
            return None, f"待核明细 {pending_id} 不存在"
        if pending.get("status") == self.PENDING_DONE_STATUS:
            # 幂等：同一行已参与过有效归并，重跑直接返回原结果，不重复归并、不重复审计
            return {
                "ok": True,
                "already_merged": True,
                "pending_id": pending_id,
                "row_ref": pending.get("row_ref"),
                "merged_row": pending.get("merged_row"),
                "delivered": pending.get("delivered", False),
                "pending": pending,
            }, f"待核明细 {pending_id} 已归并，重复重试返回原结果"
        if pending.get("status") != "已补键":
            return None, f"待核明细 {pending_id} 尚未补录归并键，不能重试归并"

        rule = store.find(self.MODULE, int(pending.get("rule_id", 0)))
        if rule is None or rule.get("status") != "已生效":
            return None, "归并规则不存在或已停用，无法重试归并"

        merge_key = pending.get("merge_key", "id")
        side = pending.get("source_side", "primary")
        key = str(pending.get("completed_key", "")).strip()
        row = dict(pending.get("row_snapshot") or {})
        row[merge_key] = key

        pairs_other: list[tuple[str, dict[str, Any]]] = []
        for other in counterpart_rows or []:
            okey, reason = self._classify_row_key(other, merge_key)
            if reason is None:
                pairs_other.append((okey, other))
        conflict = any(okey == key for okey, _ in pairs_other)

        if side == "primary":
            result = self._merge_valid_pairs(rule, [(key, row)], pairs_other)
        else:
            result = self._merge_valid_pairs(rule, pairs_other, [(key, row)])
        delivered_rows = [
            r for r in result
            if r.get("_source") == side and str(r.get(merge_key, "")).strip() == key
        ]
        merged_row = delivered_rows[0] if delivered_rows else None
        delivered = merged_row is not None

        pending["status"] = self.PENDING_DONE_STATUS
        pending["merged_row"] = merged_row
        pending["delivered"] = delivered
        pending["retry_conflict"] = conflict
        pending["merged_at"] = self._now_iso()
        write_merge_review(pending_id, pending)

        self._write_merge_audit({
            "api_name": rule.get("主API", ""),
            "event": "merge_retry_completed",
            "rule_id": pending.get("rule_id"),
            "rule_no": pending.get("rule_no", ""),
            "pending_id": pending_id,
            "affected_rows": [{
                "pending_id": pending_id,
                "row_ref": pending.get("row_ref"),
                "source_side": side,
                "reason": pending.get("reason"),
                "reason_label": pending.get("reason_label"),
            }],
            "completed_key": key,
            "conflict": conflict,
            "delivered": delivered,
            "result": "重试归并完成" if delivered else "重试归并完成（冲突按策略未交付）",
        })
        return {
            "ok": True,
            "already_merged": False,
            "pending_id": pending_id,
            "row_ref": pending.get("row_ref"),
            "merged_row": merged_row,
            "delivered": delivered,
            "conflict": conflict,
            "pending": pending,
        }, f"待核明细 {pending_id} 重试归并完成"

    # ----------------------------------------------------------
    # 待核明细查询与可交付确认
    # ----------------------------------------------------------

    def list_pending_reviews(self, rule_id: int | None = None,
                             status: str | None = None) -> list[dict[str, Any]]:
        """列出无归并键待核明细，供复核人分侧处理。"""
        from app.services.data_manager import list_merge_reviews
        return list_merge_reviews(rule_id=rule_id, status=status)

    def deliverable_summary(self, rule_id: int) -> dict[str, Any]:
        """确认哪部分数据已经可以交付：最近归并运行的有效结果 + 已补键重试交付的行。"""
        from app.services.data_manager import list_merge_reviews, list_merge_runs

        runs = list_merge_runs(rule_id=rule_id)
        latest = runs[-1] if runs else None
        reviews = list_merge_reviews(rule_id=rule_id)
        retried_rows = [r.get("merged_row") for r in reviews
                        if r.get("status") == self.PENDING_DONE_STATUS and r.get("merged_row")]
        open_reviews = [r for r in reviews if r.get("status") in self.PENDING_OPEN_STATUSES]
        run_rows = list(latest.get("merged", [])) if latest else []
        return {
            "rule_id": rule_id,
            "latest_run_id": latest.get("run_id") if latest else None,
            "merged_rows": run_rows,
            "retried_rows": retried_rows,
            "pending_reviews": open_reviews,
            "deliverable_count": len(run_rows) + len(retried_rows),
            "pending_count": len(open_reviews),
        }

    # ----------------------------------------------------------
    # 既有链路
    # ----------------------------------------------------------

    def merge_and_audit(self, rule_id: int, source_a_rows: list[dict[str, Any]],
                        source_b_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """归并两个数据源并写入审计记录。

        跨模块增强：归并完成后自动写入call_audit审计。
        """
        merged = self.merge_sources(rule_id, source_a_rows, source_b_rows)
        rule = store.find(self.MODULE, rule_id)

        # 联动增强：归并结果写入审计
        from app.services.data_manager import write_audit, DATA_ROOT
        audit_id = max((int(f.stem) for f in DATA_ROOT.glob("audit/[0-9]*.json")), default=0) + 1
        write_audit(audit_id, {
            "audit_id": audit_id,
            "api_name": rule.get("主API", "") if rule else "",
            "caller": "data_merge",
            "event": "merge_completed",
            "primary_count": len(source_a_rows),
            "secondary_count": len(source_b_rows),
            "merged_count": len(merged),
            "rule_no": rule.get("规则编号", "") if rule else "",
        })
        return merged

    def merge_from_exports(self, rule_id: int, validated: bool = True) -> Any:
        """从导出产物直接读取两个数据源并进行归并。

        跨模块增强：被 export_task 完成后调用，实现导出->归并->审计链路。
        validated=True 且规则已生效时走归并校验流程：缺键行转待核明细，
        原行身份指向导出文件行号，便于复核人回原文件找键。
        """
        from app.services.data_manager import list_exports
        exports = list_exports()
        exports.sort(key=lambda x: x.get("_created_at", ""), reverse=True)
        source_a = exports[0] if len(exports) > 0 else {}
        source_b = exports[1] if len(exports) > 1 else {}

        rows_a: list[dict[str, Any]] = []
        rows_b: list[dict[str, Any]] = []
        refs_a: list[str] = []
        refs_b: list[str] = []
        cols_a = source_a.get("columns", [])
        cols_b = source_b.get("columns", [])
        for idx, row_vals in enumerate(source_a.get("rows", [])):
            row = {col: val for col, val in zip(cols_a, row_vals)} if cols_a else {}
            rows_a.append(row)
            refs_a.append(f"export:{source_a.get('export_id', '?')}:primary:row:{idx}")
        for idx, row_vals in enumerate(source_b.get("rows", [])):
            row = {col: val for col, val in zip(cols_b, row_vals)} if cols_b else {}
            rows_b.append(row)
            refs_b.append(f"export:{source_b.get('export_id', '?')}:secondary:row:{idx}")

        rule = store.find(self.MODULE, rule_id)
        if validated and rule is not None and rule.get("status") == "已生效":
            return self.run_validated_merge(
                rule_id, rows_a, rows_b,
                row_refs={"primary": refs_a, "secondary": refs_b})
        return self.merge_and_audit(rule_id, rows_a, rows_b)

    # ----------------------------------------------------------
    # 内部工具
    # ----------------------------------------------------------

    def _write_merge_audit(self, data: dict[str, Any]) -> int:
        """写一条归并相关审计记录，返回 audit_id。"""
        from app.services.data_manager import write_audit, DATA_ROOT
        audit_id = max((int(f.stem) for f in DATA_ROOT.glob("audit/[0-9]*.json")), default=0) + 1
        write_audit(audit_id, {"audit_id": audit_id, "caller": "data_merge", **data})
        return audit_id

    @staticmethod
    def _now_iso() -> str:
        return datetime.now(timezone(timedelta(hours=8))).isoformat()

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"

        if action == "标记生效":
            from app.services.data_manager import read_api_snapshot
            registry = read_api_snapshot()
            primary_api = entry.get("主API", "")
            secondary_api = entry.get("从API", "")
            api_names = {v.get("api_name") for v in (registry.values() if isinstance(registry, dict) else []) if isinstance(v, dict)}
            if primary_api not in api_names and secondary_api not in api_names:
                return None, "至少一个源API尚未注册"

        result = super().run_action(entry_id, action)
        from app.services.data_manager import write_module_snapshot
        write_module_snapshot(self.MODULE, self.domain_snapshot())
        return result

    def domain_snapshot(self) -> dict[str, Any]:
        rows = store.rows(self.MODULE)
        merge_list = []
        for row in rows:
            merge_list.append({
                "id": row.get("id"),
                "rule_no": row.get("规则编号"),
                "primary_api": row.get("主API"),
                "secondary_api": row.get("从API"),
                "merge_key": row.get("归并键"),
                "conflict_policy": row.get("冲突策略"),
                "dedup_policy": row.get("去重策略"),
                "sort": row.get("结果排序"),
                "version": row.get("规则版本"),
                "status": row.get("status"),
            })
        from app.services.data_manager import list_merge_reviews
        open_pending = sum(
            1 for r in list_merge_reviews() if r.get("status") in self.PENDING_OPEN_STATUSES)
        return {
            "module": self.MODULE,
            "entity": self.ENTITY,
            "artifact": f"{self.ENTITY}数据集",
            "count": len(merge_list),
            "merges": merge_list,
            "active_count": sum(1 for r in merge_list if r.get("status") == "已生效"),
            "open_pending_review_count": open_pending,
        }
