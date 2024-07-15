import { isNil, isEmpty } from "lodash";
import { useMemo } from "react";
import { currentUser } from "@/services/auth";
import { policy } from "@/services/policy";

export default function useQueryFlags(query, dataSource = null) {
  dataSource = dataSource || { view_only: true };

  return useMemo(
    () => ({
      // state flags
      isNew: isNil(query.id),
      isDraft: query.is_draft,
      isArchived: query.is_archived,

      // permissions flags
      canCreate: currentUser.hasPermission("create_query"),
      canView: currentUser.hasPermission("view_query"),
      canEdit: currentUser.hasPermission("edit_query") && policy.canEdit(query),
      canViewSource: currentUser.hasPermission("view_source"),
      canExecute:
        !isEmpty(query.query) &&
        policy.canExecuteQuery() &&
        (query.is_safe || !dataSource.view_only),
      canFork: policy.canForkQuery() && !dataSource.view_only,
      canSchedule: policy.canSchedule(),
      canSave: policy.canSaveQuery(),
      canRegenerateApiKey: policy.canRegenerateApiKey(),
      canEditVisualization: policy.canEditVisualization(),
      canEditDashboard: policy.canEditDashboard()
    }),
    [query, dataSource.view_only]
  );
}
