// src/components/NewsGraph.tsx
import React from "react";
import ForceGraph3D from "react-force-graph-3d";

export type NewsNode = {
  id: string;
  url: string;
  title: string | null;
  published_at: string | null;
  domain: string | null;
  language: string | null;
  topic: string;
  val: number; // node size
};

type GraphData = {
  nodes: NewsNode[];
  links: Array<{ source: string; target: string; weight?: number }>;
};

type Props = { nodes: NewsNode[] };

export default function NewsGraph({ nodes }: Props) {
  const data: GraphData = { nodes, links: [] };

  return (
    <div className="h-[80vh] rounded-2xl overflow-hidden ring-1 ring-zinc-800">
      <ForceGraph3D
        graphData={data}
        nodeId={"id"}
        nodeVal={(n) => (n as NewsNode).val}
        nodeAutoColorBy={"topic"}
        nodeLabel={(n) => {
          const nn = n as NewsNode;
          return `${nn.title ?? "(no title)"}\n${nn.published_at ?? ""}`;
        }}
        onNodeClick={(n) => window.open((n as NewsNode).url, "_blank")}
        backgroundColor="#0b1020"
      />
    </div>
  );
}
