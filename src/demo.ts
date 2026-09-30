export const rooms = [
  {
    id: "development", name: "Development", messages: [
      "The login flow is complete. All checks passed and the changes are ready for review.",
      "The agent fixed the checkout bug and added a regression test.",
      "The dashboard is underway. The layout is ready; live data is next.",
    ],
  },
  {
    id: "code-review", name: "Code Review", messages: [
      "The review is complete. No blocking issues were found.",
      "The reviewer found a missing error handler. The agent is fixing it.",
      "Two naming changes were suggested to make the code easier to read.",
    ],
  },
  {
    id: "deployments", name: "Deployments", messages: [
      "The latest release is live. Production health checks passed.",
      "The staging deployment is ready for testing.",
      "The deployment failed during the build. The previous version is still running.",
    ],
  },
  {
    id: "needs-input", name: "Needs Your Input", messages: [
      "The agent needs you to choose between the two proposed layouts.",
      "The implementation is ready. Please review the changes before deployment.",
      "A required API credential is missing. Add it to continue.",
    ],
  },
];
