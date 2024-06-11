import { isArray, intersection } from "lodash";
import { useCallback } from "react";
import ScheduleDialog from "@/components/queries/ScheduleDialog";
import { clientConfig } from "@/services/auth";
import { policy } from "@/services/policy";
import useUpdateQuerySchedule from "./useUpdateQuerySchedule";
import useQueryFlags from "./useQueryFlags";
import recordEvent from "@/services/recordEvent";

export default function useEditScheduleDialog(query, onChange) {
  // We won't use flags that depend on data source
  const queryFlags = useQueryFlags(query);

  const updateQuerySchedule = useUpdateQuerySchedule(query, onChange);

  return useCallback(() => {
    if (!queryFlags.canEdit || !queryFlags.canSchedule) {
      return;
    }

    const intervals = clientConfig.queryRefreshIntervals;
    const allowedIntervals = policy.getQueryRefreshIntervals();
    const refreshOptions = isArray(allowedIntervals) ? intersection(intervals, allowedIntervals) : intervals;

    ScheduleDialog.showModal({
      schedule: query.schedule,
      refreshOptions,
    }).onClose(schedule => {
      recordEvent("edit_schedule", "query", query.id);
      updateQuerySchedule({ schedule });
    });
  }, [query.id, query.schedule, queryFlags.canEdit, queryFlags.canSchedule, updateQuerySchedule]);
}
