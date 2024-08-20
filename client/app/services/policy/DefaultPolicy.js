import { get, isArray } from "lodash";
import { currentUser, clientConfig } from "@/services/auth";

/* eslint-disable class-methods-use-this */

export default class DefaultPolicy {
  refresh() {
    return Promise.resolve(this);
  }

  canCreateDataSource() {
    return currentUser.isAdmin;
  }

  isCreateDataSourceEnabled() {
    return currentUser.isAdmin;
  }

  canCreateDestination() {
    return currentUser.isAdmin;
  }

  isCreateDestinationEnabled() {
    return currentUser.isAdmin;
  }

  canCreateDashboard() {
    return currentUser.hasPermission("create_dashboard");
  }

  isCreateDashboardEnabled() {
    return currentUser.hasPermission("create_dashboard");
  }

  canCreateAlert() {
    return true;
  }

  canCreateUser() {
    return currentUser.isAdmin;
  }

  isCreateUserEnabled() {
    return currentUser.isAdmin;
  }

  isCreateQuerySnippetEnabled() {
    return true;
  }

  getDashboardRefreshIntervals() {
    const result = clientConfig.dashboardRefreshIntervals;
    return isArray(result) ? result : null;
  }

  getQueryRefreshIntervals() {
    const result = clientConfig.queryRefreshIntervals;
    return isArray(result) ? result : null;
  }

  //esto maneja los permisos por query y dashboard para editor
  canEdit(object) {
    return get(object, "can_edit", false);
  }

  canRun() {
    return true;
  }

  canSaveQuery() {
    return currentUser.hasPermission("save_query");
  }

  canRegenerateApiKey() {
    return currentUser.hasPermission("regenerate_api_query");
  }

  canForkQuery() {
    return currentUser.hasPermission("fork_query");
  }

  canExecuteQuery() {
    return currentUser.hasPermission("execute_query");
  }

  canSchedule() {
    return currentUser.hasPermission("edit_query_schedule") && currentUser.hasPermission("schedule_query");
  }

  canEditVisualization() {
    return currentUser.hasPermission("edit_visualization_query");
  }

  canEditDashboard() {
    return currentUser.hasPermission("edit_dashboard");
  }

}
