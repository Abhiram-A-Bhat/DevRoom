import Docker from "dockerode";

// Windows uses named pipes for Docker
// On Linux/Mac, use: { socketPath: '/var/run/docker.sock' }
const docker = new Docker({
  socketPath: process.env.DOCKER_SOCKET || "//./pipe/docker_engine",
});

interface LanguageConfig {
  image: string;
  filename: string;
  runCmd: string;
  timeoutMs: number;
}

const LANGUAGE_CONFIGS: Record<string, LanguageConfig> = {
  python: {
    image: "python:3.11-alpine",
    filename: "solution.py",
    runCmd: "python solution.py",
    timeoutMs: 10_000,
  },
  javascript: {
    image: "node:20-alpine",
    filename: "solution.js",
    runCmd: "node solution.js",
    timeoutMs: 10_000,
  },
  java: {
    image: "eclipse-temurin:21-jdk-alpine",
    filename: "Solution.java",
    runCmd: "javac Solution.java && java Solution",
    timeoutMs: 20_000,
  },
};

export interface ExecutionResult {
  stdout: string;
  stderr: string;
  exitCode: number;
  durationMs: number;
  status: "success" | "error" | "timeout";
}

const MAX_STDOUT_BYTES = 10_000;
const MAX_STDERR_BYTES = 5_000;

function pullImage(image: string): Promise<void> {
  return new Promise((resolve, reject) => {
    docker.pull(image, (err: Error | null, stream?: NodeJS.ReadableStream) => {
      if (err || !stream) return reject(err ?? new Error(`Could not pull ${image}`));
      docker.modem.followProgress(stream, (progressErr: Error | null) =>
        progressErr ? reject(progressErr) : resolve(),
      );
    });
  });
}

export async function prepareImages(): Promise<void> {
  for (const { image } of Object.values(LANGUAGE_CONFIGS)) {
    console.log(`[WORKER] Pulling ${image}`);
    await pullImage(image);
  }
}

export async function executeCode(
  language: string,
  code: string,
): Promise<ExecutionResult> {
  const config = LANGUAGE_CONFIGS[language];
  if (!config) throw new Error(`Unsupported language: ${language}`);

  const startTime = Date.now();
  let container: Docker.Container | null = null;

  // Base64 keeps user code from being interpreted by the wrapper shell
  const encodedCode = Buffer.from(code, "utf8").toString("base64");

  try {
    container = await docker.createContainer({
      Image: config.image,
      Cmd: [
        "sh",
        "-c",
        `printf %s '${encodedCode}' | base64 -d > ${config.filename} && ${config.runCmd}`,
      ],
      User: "65534:65534",
      WorkingDir: "/sandbox",
      AttachStdout: true,
      AttachStderr: true,
      Tty: false,
      NetworkDisabled: true, // No internet — security
      HostConfig: {
        Memory: 128 * 1024 * 1024, // 128 MB RAM cap
        CpuPeriod: 100_000,
        CpuQuota: 50_000, // 50% CPU cap
        MemorySwap: 128 * 1024 * 1024, // No swap beyond the RAM cap
        PidsLimit: 64, // Stops fork bombs
        CapDrop: ["ALL"],
        SecurityOpt: ["no-new-privileges"],
        ReadonlyRootfs: true,
        Tmpfs: {
          "/sandbox": "rw,nosuid,nodev,size=16m,uid=65534,gid=65534,mode=700",
          "/tmp": "rw,nosuid,nodev,size=16m,uid=65534,gid=65534,mode=700",
        },
        Ulimits: [{ Name: "nofile", Soft: 256, Hard: 256 }],
      },
    });

    await container.start();

    // Race: execution finishes OR timeout
    let timer: NodeJS.Timeout | undefined;
    const result = await Promise.race([
      collectOutput(container),
      new Promise<"timeout">((resolve) => {
        timer = setTimeout(() => resolve("timeout"), config.timeoutMs);
      }),
    ]).finally(() => clearTimeout(timer));

    const durationMs = Date.now() - startTime;

    if (result === "timeout") {
      try {
        await container.kill();
      } catch {}
      return {
        stdout: "",
        stderr: "Execution timed out",
        exitCode: -1,
        durationMs,
        status: "timeout",
      };
    }

    return {
      stdout: result.stdout,
      stderr: result.stderr,
      exitCode: result.exitCode,
      durationMs,
      status: result.exitCode === 0 ? "success" : "error",
    };
  } catch (err: any) {
    return {
      stdout: "",
      stderr: err.message || "Execution failed",
      exitCode: -1,
      durationMs: Date.now() - startTime,
      status: "error",
    };
  } finally {
    // Removed here, not with AutoRemove, so wait() can still read the exit code
    await container?.remove({ force: true }).catch(() => {});
  }
}

async function collectOutput(
  container: Docker.Container,
): Promise<{ stdout: string; stderr: string; exitCode: number }> {
  const stream = await container.logs({
    stdout: true,
    stderr: true,
    follow: true,
  });

  let stdoutBuf = Buffer.alloc(0);
  let stderrBuf = Buffer.alloc(0);
  let raw = Buffer.alloc(0);

  return new Promise((resolve, reject) => {
    stream.on("data", (chunk: Buffer) => {
      raw = Buffer.concat([raw, chunk]);

      // Docker stream format:
      // [8-byte header][payload]
      // header[0] = stream type: 1=stdout, 2=stderr
      // header[4..8] = payload length (big-endian uint32)
      while (raw.length >= 8) {
        const type = raw[0];
        const size = raw.readUInt32BE(4);

        if (raw.length < 8 + size) break; // wait for more data

        const payload = raw.subarray(8, 8 + size);
        if (type === 1 && stdoutBuf.length < MAX_STDOUT_BYTES) {
          stdoutBuf = Buffer.concat([stdoutBuf, payload]).subarray(0, MAX_STDOUT_BYTES);
        } else if (type === 2 && stderrBuf.length < MAX_STDERR_BYTES) {
          stderrBuf = Buffer.concat([stderrBuf, payload]).subarray(0, MAX_STDERR_BYTES);
        }

        raw = raw.subarray(8 + size);
      }
    });

    stream.on("end", async () => {
      try {
        const data = await container.wait();
        resolve({
          stdout: stdoutBuf.toString("utf8"),
          stderr: stderrBuf.toString("utf8"),
          exitCode: data.StatusCode,
        });
      } catch (err) {
        reject(err);
      }
    });

    stream.on("error", reject);
  });
}
