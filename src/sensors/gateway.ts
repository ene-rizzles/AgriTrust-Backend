import * as net from 'net';
import { Transform, TransformCallback } from 'stream';
import { SlidingWindowBuffer, TelemetryFrame } from './buffer_manager';
import { backpressure, BackpressureLevel } from './backpressure';

const DEFAULT_PORT = 4000;

function createSensorGateway(buffer: SlidingWindowBuffer, port: number = DEFAULT_PORT): net.Server {
  const server = net.createServer((socket) => {
    const sensorId = `${socket.remoteAddress}:${socket.remotePort}`;

    const transform = new Transform({
      objectMode: true,
      transform(chunk: Buffer, _encoding: string, callback: TransformCallback) {
        if (backpressure.globalBackpressure) {
          socket.pause();
          backpressure.setBackpressure(sensorId, BackpressureLevel.CRITICAL);
          const checkInterval = setInterval(() => {
            if (!backpressure.globalBackpressure) {
              clearInterval(checkInterval);
              socket.resume();
              backpressure.setBackpressure(sensorId, BackpressureLevel.NORMAL);
            }
          }, 50);
          callback(null);
          return;
        }

        const frame: TelemetryFrame = {
          sensorId,
          payload: Buffer.from(chunk),
          timestamp: new Date(),
        };

        const accepted = buffer.write(frame);
        if (!accepted) {
          backpressure.setBackpressure(sensorId, BackpressureLevel.WARNING);
        } else {
          backpressure.setBackpressure(sensorId, BackpressureLevel.NORMAL);
        }

        callback(null);
      },
    });

    socket.pipe(transform);

    socket.on('error', (err) => {
      console.error(`Sensor socket error [${sensorId}]:`, err.message);
    });

    socket.on('close', () => {
      backpressure.setBackpressure(sensorId, BackpressureLevel.NORMAL);
    });
  });

  return server;
}

export { createSensorGateway, DEFAULT_PORT };
