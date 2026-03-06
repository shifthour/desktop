-- CreateTable
CREATE TABLE `Physiotherapist` (
    `id` VARCHAR(191) NOT NULL,
    `slug` VARCHAR(191) NOT NULL,
    `name` VARCHAR(191) NOT NULL,
    `photo` VARCHAR(191) NOT NULL DEFAULT '',
    `gender` VARCHAR(191) NOT NULL,
    `experience` INTEGER NOT NULL,
    `bio` TEXT NOT NULL,
    `rating` DOUBLE NOT NULL DEFAULT 0,
    `reviewCount` INTEGER NOT NULL DEFAULT 0,
    `clinicName` VARCHAR(191) NOT NULL,
    `locationArea` VARCHAR(191) NOT NULL,
    `locationCity` VARCHAR(191) NOT NULL,
    `locationLat` DOUBLE NOT NULL,
    `locationLng` DOUBLE NOT NULL,
    `verified` BOOLEAN NOT NULL DEFAULT false,
    `totalSessions` INTEGER NOT NULL DEFAULT 0,
    `createdAt` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    `updatedAt` DATETIME(3) NOT NULL,

    UNIQUE INDEX `Physiotherapist_slug_key`(`slug`),
    INDEX `Physiotherapist_slug_idx`(`slug`),
    INDEX `Physiotherapist_locationCity_idx`(`locationCity`),
    INDEX `Physiotherapist_rating_idx`(`rating`),
    PRIMARY KEY (`id`)
) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- CreateTable
CREATE TABLE `Specialization` (
    `id` VARCHAR(191) NOT NULL,
    `name` VARCHAR(191) NOT NULL,
    `icon` VARCHAR(191) NOT NULL DEFAULT '',
    `count` INTEGER NOT NULL DEFAULT 0,
    `image` VARCHAR(191) NOT NULL DEFAULT '',

    UNIQUE INDEX `Specialization_name_key`(`name`),
    PRIMARY KEY (`id`)
) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- CreateTable
CREATE TABLE `PhysioSpecialization` (
    `physioId` VARCHAR(191) NOT NULL,
    `specializationId` VARCHAR(191) NOT NULL,

    PRIMARY KEY (`physioId`, `specializationId`)
) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- CreateTable
CREATE TABLE `Qualification` (
    `id` VARCHAR(191) NOT NULL,
    `physioId` VARCHAR(191) NOT NULL,
    `text` VARCHAR(191) NOT NULL,
    `sortOrder` INTEGER NOT NULL DEFAULT 0,

    INDEX `Qualification_physioId_idx`(`physioId`),
    PRIMARY KEY (`id`)
) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- CreateTable
CREATE TABLE `PhysioVisitType` (
    `physioId` VARCHAR(191) NOT NULL,
    `visitType` VARCHAR(191) NOT NULL,

    PRIMARY KEY (`physioId`, `visitType`)
) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- CreateTable
CREATE TABLE `Service` (
    `id` VARCHAR(191) NOT NULL,
    `physioId` VARCHAR(191) NOT NULL,
    `name` VARCHAR(191) NOT NULL,
    `duration` INTEGER NOT NULL,
    `price` DOUBLE NOT NULL,
    `description` TEXT NOT NULL,
    `sortOrder` INTEGER NOT NULL DEFAULT 0,

    INDEX `Service_physioId_idx`(`physioId`),
    PRIMARY KEY (`id`)
) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- CreateTable
CREATE TABLE `Review` (
    `id` VARCHAR(191) NOT NULL,
    `physioId` VARCHAR(191) NOT NULL,
    `patientName` VARCHAR(191) NOT NULL,
    `rating` INTEGER NOT NULL,
    `date` VARCHAR(191) NOT NULL,
    `text` TEXT NOT NULL,
    `verified` BOOLEAN NOT NULL DEFAULT false,

    INDEX `Review_physioId_idx`(`physioId`),
    PRIMARY KEY (`id`)
) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- CreateTable
CREATE TABLE `Booking` (
    `id` VARCHAR(191) NOT NULL,
    `physioId` VARCHAR(191) NOT NULL,
    `physioName` VARCHAR(191) NOT NULL,
    `patientName` VARCHAR(191) NOT NULL,
    `patientEmail` VARCHAR(191) NOT NULL,
    `patientPhone` VARCHAR(191) NOT NULL,
    `serviceId` VARCHAR(191) NOT NULL,
    `serviceName` VARCHAR(191) NOT NULL,
    `date` VARCHAR(191) NOT NULL,
    `time` VARCHAR(191) NOT NULL,
    `duration` INTEGER NOT NULL,
    `fee` DOUBLE NOT NULL,
    `platformFee` DOUBLE NOT NULL,
    `status` VARCHAR(191) NOT NULL DEFAULT 'pending',
    `reason` VARCHAR(191) NULL,
    `createdAt` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),

    INDEX `Booking_physioId_idx`(`physioId`),
    INDEX `Booking_status_idx`(`status`),
    PRIMARY KEY (`id`)
) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- AddForeignKey
ALTER TABLE `PhysioSpecialization` ADD CONSTRAINT `PhysioSpecialization_physioId_fkey` FOREIGN KEY (`physioId`) REFERENCES `Physiotherapist`(`id`) ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE `PhysioSpecialization` ADD CONSTRAINT `PhysioSpecialization_specializationId_fkey` FOREIGN KEY (`specializationId`) REFERENCES `Specialization`(`id`) ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE `Qualification` ADD CONSTRAINT `Qualification_physioId_fkey` FOREIGN KEY (`physioId`) REFERENCES `Physiotherapist`(`id`) ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE `PhysioVisitType` ADD CONSTRAINT `PhysioVisitType_physioId_fkey` FOREIGN KEY (`physioId`) REFERENCES `Physiotherapist`(`id`) ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE `Service` ADD CONSTRAINT `Service_physioId_fkey` FOREIGN KEY (`physioId`) REFERENCES `Physiotherapist`(`id`) ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE `Review` ADD CONSTRAINT `Review_physioId_fkey` FOREIGN KEY (`physioId`) REFERENCES `Physiotherapist`(`id`) ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE `Booking` ADD CONSTRAINT `Booking_physioId_fkey` FOREIGN KEY (`physioId`) REFERENCES `Physiotherapist`(`id`) ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE `Booking` ADD CONSTRAINT `Booking_serviceId_fkey` FOREIGN KEY (`serviceId`) REFERENCES `Service`(`id`) ON DELETE CASCADE ON UPDATE CASCADE;
