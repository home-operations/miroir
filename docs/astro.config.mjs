import starlight from "@astrojs/starlight";
import { defineConfig } from "astro/config";
import mermaid from "astro-mermaid";
import starlightLinksValidator from "starlight-links-validator";

export default defineConfig({
  site: "https://miroir.home-operations.com",
  integrations: [
    // Must precede starlight so its markdown transform sees the mermaid fences first.
    mermaid(),
    starlight({
      title: "miroir",
      description:
        "Replicated block storage for small Kubernetes clusters: CSI on LVM thin, ZFS, or loopfile, with synchronous DRBD9 replication.",
      social: [
        {
          icon: "github",
          label: "GitHub",
          href: "https://github.com/home-operations/miroir",
        },
      ],
      editLink: {
        baseUrl: "https://github.com/home-operations/miroir/edit/main/docs/",
      },
      sidebar: [
        "index",
        { label: "Getting started", items: ["requirements", "quickstart"] },
        { label: "Concepts", items: ["replication", "remote-consumers", "resilience"] },
        {
          label: "User guide",
          items: [
            "rwx",
            "upgrading",
            "maintenance",
            "monitoring",
            "configuration",
            "coexistence",
            "troubleshooting",
            "uninstall",
          ],
        },
        "development",
      ],
      plugins: [starlightLinksValidator()],
    }),
  ],
});
