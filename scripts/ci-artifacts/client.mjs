// Same official client used by actions/upload-artifact@v7. Current-run scope only.
import {DefaultArtifactClient} from '@actions/artifact';
export const client = new DefaultArtifactClient();
