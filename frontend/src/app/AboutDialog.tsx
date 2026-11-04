import { ExternalLink } from 'lucide-react';
import { ButtonAnchor } from '@/components/ui/button';
import { Skeleton } from '@/components/ui/feedback';
import { KeyValue } from '@/components/ui/layout';
import { Modal } from '@/components/ui/overlays';
import { REPO_URL, SERVER_LICENSES_URL, useSystemInfo, WEB_LICENSES_URL } from '@/lib/about';

/** Help → About BeziSync: the server's version, BeziSync's license and the third-party notices. */
export function AboutDialog({ open, onOpenChange }: { open: boolean; onOpenChange: (open: boolean) => void }) {
  const info = useSystemInfo();
  const version = info.data ? (
    <span className="font-mono">{info.data.version}</span>
  ) : info.isError ? (
    <span className="text-muted">Unknown</span>
  ) : (
    <Skeleton lines={1} className="w-24" />
  );
  return (
    <Modal open={open} onOpenChange={onOpenChange} size="sm" title="About BeziSync" description="A self-service control plane for Debezium Server.">
      <KeyValue
        items={[
          ['Version', version],
          ['License', 'Apache License 2.0'],
        ]}
      />
      <p className="m-0 text-base text-ink-2">
        BeziSync is open source. Its code, issues and releases are on{' '}
        <a href={REPO_URL} target="_blank" rel="noreferrer" className="font-semibold text-accent hover:underline">
          GitHub
        </a>
        .
      </p>
      <div className="flex flex-col gap-2">
        <p className="m-0 text-base text-ink-2">It includes open-source software from other projects. Their licenses and notices:</p>
        <div className="flex flex-wrap gap-2">
          <ButtonAnchor href={WEB_LICENSES_URL} target="_blank" rel="noreferrer" size="sm" icon={<ExternalLink />}>
            Web app licenses
          </ButtonAnchor>
          <ButtonAnchor href={SERVER_LICENSES_URL} target="_blank" rel="noreferrer" size="sm" icon={<ExternalLink />}>
            Server licenses
          </ButtonAnchor>
        </div>
      </div>
      <p className="m-0 text-sm text-muted">Debezium Server is a separate project under its own license. BeziSync runs it but doesn&apos;t include it.</p>
    </Modal>
  );
}
